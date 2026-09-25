// Package gateway 在本地 TCP 监听器之上并发接入多个 stream session，
// 统一实施全局连接/字节配额与停机生命周期：
//
//   - 超过全局连接数：accept 后立即以可区分原因拒绝（拒绝帧可选发送）；
//   - 停机期间：监听器先关闭，新连接在 OS 层即被拒；竞态中已 accept 的
//     连接被标记为“拒入”，不会进入运行态；
//   - Shutdown：停止接入 → 对在途会话发起宽限排空，超时统一强制终止；
//   - 重复 Listen/Serve/Shutdown 幂等，不 panic、不回退状态。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
	"github.com/lisama569/stream-session-gateway-go/pkg/session"
)

// ErrClosed 表示网关已关闭/停机期间被调用。
var ErrClosed = errors.New("gateway: closed")

// ErrAlreadyListening 表示重复 Listen。
var ErrAlreadyListening = errors.New("gateway: already listening")

// Config 网关可配置项。
type Config struct {
	Listen string // 本地监听地址，默认 127.0.0.1:0（随机端口）

	MaxConnections int   // 全局并发连接数；<=0 不限
	GlobalBytes    int64 // 全局所有会话在途+待写字节总预算；<=0 不限
	Session        session.Options

	ShutdownGrace time.Duration // Shutdown 默认宽限；<=0 使用 5s

	// OnSessionEnd 在每个会话终结后回调（状态与原因快照），可观测/测试用。
	OnSessionEnd func(id uint64, st session.State, reason session.CloseReason)
}

// Stats 是网关计数器快照。
type Stats struct {
	Accepted              int64 // 成功进入运行态的连接数
	RejectedConnQuota     int64 // 因全局连接配额被拒
	RejectedWhileShutdown int64 // 停机竞态中被拒
}

// Gateway 本地双向流会话网关。
type Gateway struct {
	cfg     Config
	handler session.Handler

	mu        sync.Mutex
	ln        net.Listener
	listening bool
	closed    bool

	gate  *quota.Gate
	bytes *quota.Limiter

	idCounter atomic.Uint64
	accepted  atomic.Int64
	rejConn   atomic.Int64
	rejDown   atomic.Int64

	sessionsMu sync.Mutex
	sessions   map[uint64]*session.Session

	wg sync.WaitGroup // 所有会话 Run

	logf func(string, ...any)
}

// New 创建网关；不开始监听。
func New(h session.Handler, cfg Config) *Gateway {
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 5 * time.Second
	}
	return &Gateway{
		cfg:      cfg,
		handler:  h,
		gate:     quota.NewGate(cfg.MaxConnections),
		bytes:    quota.NewLimiter(cfg.GlobalBytes),
		sessions: make(map[uint64]*session.Session),
		logf:     func(string, ...any) {},
	}
}

// SetLogger 注入日志函数（默认静默）。
func (g *Gateway) SetLogger(f func(string, ...any)) {
	if f != nil {
		g.logf = f
	}
}

// ListenAddr 返回实际监听地址（Listen 后有效）。
func (g *Gateway) ListenAddr() net.Addr {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ln == nil {
		return nil
	}
	return g.ln.Addr()
}

// ActiveSessions 返回当前活跃会话数快照。
func (g *Gateway) ActiveSessions() int {
	g.sessionsMu.Lock()
	defer g.sessionsMu.Unlock()
	return len(g.sessions)
}

// Stats 返回计数器快照。
func (g *Gateway) Stats() Stats {
	return Stats{
		Accepted:              g.accepted.Load(),
		RejectedConnQuota:     g.rejConn.Load(),
		RejectedWhileShutdown: g.rejDown.Load(),
	}
}

// Listen 在本地地址建立监听器。
func (g *Gateway) Listen() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrClosed
	}
	if g.ln != nil {
		return ErrAlreadyListening
	}
	ln, err := net.Listen("tcp", g.cfg.Listen)
	if err != nil {
		return fmt.Errorf("gateway listen: %w", err)
	}
	g.ln = ln
	g.listening = true
	return nil
}

// Start 等价 Listen + 后台 Serve。
func (g *Gateway) Start(ctx context.Context) error {
	if err := g.Listen(); err != nil {
		return err
	}
	go func() { _ = g.Serve(ctx) }()
	return nil
}

// Serve 接受连接循环，阻塞直到监听器关闭（Shutdown）或 ctx 取消。
func (g *Gateway) Serve(ctx context.Context) error {
	g.mu.Lock()
	ln := g.ln
	g.mu.Unlock()
	if ln == nil {
		return errors.New("gateway: Listen not called")
	}

	// ctx 取消时关闭监听器以解除 Accept。
	ctxDone := make(chan struct{})
	defer close(ctxDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = g.beginShutdown()
		case <-ctxDone:
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			g.mu.Lock()
			closed := g.closed
			g.mu.Unlock()
			if closed {
				return nil // 正常停机
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("gateway accept: %w", err)
		}
		g.handleConn(conn)
	}
}

// handleConn 执行“拒入检查 → 注册 → 运行 → 注销”，全程无锁等待。
func (g *Gateway) handleConn(conn net.Conn) {
	// 停机竞态：即便在 Shutdown 关闭监听器的同一瞬间 accept 成功，
	// closed 检查也保证它不会进入运行态。
	g.mu.Lock()
	closed := g.closed
	g.mu.Unlock()
	if closed {
		g.rejDown.Add(1)
		_ = conn.Close()
		g.logf("场景=停机期间拒入 远端=%s 判定依据=closed标志", conn.RemoteAddr())
		return
	}
	if !g.gate.TryEnter() {
		// 全局连接配额拒绝：可区分原因（RejectedConnQuota），立即关闭不悬挂。
		g.rejConn.Add(1)
		g.logf("场景=全局连接配额拒绝 远端=%s", conn.RemoteAddr())
		_ = conn.Close()
		return
	}

	id := g.idCounter.Add(1)
	sess := session.New(id, conn, g.handler, g.cfg.Session)
	sess.SetGlobalLimiter(g.bytes)

	g.sessionsMu.Lock()
	g.sessions[id] = sess
	g.sessionsMu.Unlock()
	g.accepted.Add(1)
	g.logf("场景=接入 id=%d 远端=%s 活跃=%d", id, conn.RemoteAddr(), g.ActiveSessions())

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		_ = sess.Run(context.Background())
		g.sessionsMu.Lock()
		delete(g.sessions, id)
		g.sessionsMu.Unlock()
		g.gate.Leave()
		if g.cfg.OnSessionEnd != nil {
			g.cfg.OnSessionEnd(id, sess.State(), sess.Reason())
		}
		g.logf("场景=会话结束 id=%d state=%s reason=%s 活跃=%d",
			id, sess.State(), sess.Reason(), g.ActiveSessions())
	}()
}

// beginShutdown 幂等地关闭监听器并标记 closed（不等待会话排空）。
func (g *Gateway) beginShutdown() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	ln := g.ln
	g.listening = false
	g.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	g.logf("场景=网关停止接入")
	return nil
}

// Shutdown 停止接收新连接，并对在途会话执行宽限排空；
// 宽限到期仍有会话未结束则统一强制终止。幂等：重复调用等待同一结果。
func (g *Gateway) Shutdown(grace time.Duration) error {
	if grace <= 0 {
		grace = g.cfg.ShutdownGrace
	}
	_ = g.beginShutdown()

	g.sessionsMu.Lock()
	snapshot := make([]*session.Session, 0, len(g.sessions))
	for _, s := range g.sessions {
		snapshot = append(snapshot, s)
	}
	g.sessionsMu.Unlock()

	g.logf("场景=发起宽限排空 在途会话=%d grace=%s", len(snapshot), grace)
	for _, s := range snapshot {
		_ = s.GracefulClose(grace)
	}

	// 等待所有会话结束；到点未结束者强制终止。
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		g.logf("场景=网关已完全关闭")
		return nil
	case <-time.After(grace + 100*time.Millisecond):
		for _, s := range snapshot {
			_ = s.ForceClose(session.ReasonGatewayDown)
		}
		<-done
		g.logf("场景=网关宽限耗尽，已强制终止剩余会话")
		return nil
	}
}

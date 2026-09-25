package gateway

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Gateway 接收本地 TCP 连接并为每条连接创建 Session。
type Gateway struct {
	cfg    Config
	codec  *Codec
	logger Logger

	listener net.Listener

	mu           sync.RWMutex
	sessions     map[string]*Session
	shuttingDown bool

	closeOnce sync.Once
	done      chan struct{} // ListenAndServe 退出时关闭
	serveErr  *Error

	// 全局配额：并发会话数与全局在途字节数。
	globalConcurrent *Limiter
	globalBytes      *Limiter

	// acceptWg 跟踪所有会话 goroutine，供 ForceShutdown 统计，
	// 优雅停机时不等待它（改用语义化 drain）。
	sessionWg sync.WaitGroup

	// forceCtx 在强制终止（或宽限到期强制收尾）时取消，用于解除
	// PolicyBlock 下阻塞在配额申请上的读 goroutine，避免 goroutine 泄漏。
	forceCtx    context.Context
	forceCancel context.CancelFunc
	forceOnce   sync.Once
}

// New 根据配置创建网关（合并默认值并校验）。
func New(cfg Config) (*Gateway, error) {
	d := defaultConfig()
	if cfg.ListenAddr != "" {
		d.ListenAddr = cfg.ListenAddr
	}
	if cfg.MaxPayloadV1 > 0 {
		d.MaxPayloadV1 = cfg.MaxPayloadV1
	}
	if cfg.MaxPayloadV2 > 0 {
		d.MaxPayloadV2 = cfg.MaxPayloadV2
	}
	if cfg.GracePeriod > 0 {
		d.GracePeriod = cfg.GracePeriod
	}
	if cfg.ReadBuffer > 0 {
		d.ReadBuffer = cfg.ReadBuffer
	}
	d.IdleTimeout = cfg.IdleTimeout
	// 配额整型字段：负数=未设置用默认值；0=显式不限；正数=给定大小。
	if cfg.Quotas.GlobalConcurrent >= 0 {
		d.Quotas.GlobalConcurrent = cfg.Quotas.GlobalConcurrent
	}
	if cfg.Quotas.GlobalInflightBytes >= 0 {
		d.Quotas.GlobalInflightBytes = cfg.Quotas.GlobalInflightBytes
	}
	if cfg.Quotas.SessionInflightFrames >= 0 {
		d.Quotas.SessionInflightFrames = cfg.Quotas.SessionInflightFrames
	}
	if cfg.Quotas.SessionInflightBytes >= 0 {
		d.Quotas.SessionInflightBytes = cfg.Quotas.SessionInflightBytes
	}
	if cfg.Quotas.Policy == PolicyDefault {
		d.Quotas.Policy = PolicyBlock
	} else {
		d.Quotas.Policy = cfg.Quotas.Policy
	}
	// 把未设置（-1）的配额替换为内置默认值。
	if d.Quotas.GlobalConcurrent < 0 {
		d.Quotas.GlobalConcurrent = 64
	}
	if d.Quotas.GlobalInflightBytes < 0 {
		d.Quotas.GlobalInflightBytes = 32 << 20
	}
	if d.Quotas.SessionInflightFrames < 0 {
		d.Quotas.SessionInflightFrames = 1024
	}
	if d.Quotas.SessionInflightBytes < 0 {
		d.Quotas.SessionInflightBytes = 4 << 20
	}
	d.Handler = cfg.Handler
	if cfg.Logger != nil {
		d.Logger = cfg.Logger
	}
	if err := d.validate(); err != nil {
		return nil, err
	}

	g := &Gateway{
		cfg:      d,
		codec:    NewCodec(d.MaxPayloadV1, d.MaxPayloadV2),
		logger:   d.Logger,
		sessions: make(map[string]*Session),
		done:     make(chan struct{}),
	}
	g.forceCtx, g.forceCancel = context.WithCancel(context.Background())
	if d.Quotas.GlobalConcurrent > 0 {
		g.globalConcurrent = newLimiter("global-concurrent",
			int64(d.Quotas.GlobalConcurrent), PolicyReject, KindGlobalConcurrent)
	}
	if d.Quotas.GlobalInflightBytes > 0 {
		g.globalBytes = newLimiter("global-inflight-bytes",
			d.Quotas.GlobalInflightBytes, d.Quotas.Policy, KindGlobalBytes)
	}
	return g, nil
}

// Addr 返回实际监听地址（ListenAndServe 启动后有效）。
func (g *Gateway) Addr() net.Addr {
	g.mu.RLock()
	l := g.listener
	g.mu.RUnlock()
	if l == nil {
		return nil
	}
	return l.Addr()
}

func (g *Gateway) isShuttingDown() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.shuttingDown
}

// ListenAndServe 阻塞运行接入循环，直到 Shutdown/ForceShutdown 或监听出错。
// 重复调用返回首次的退出结果。
func (g *Gateway) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", g.cfg.ListenAddr)
	if err != nil {
		return newError(KindConnIO, "listen", "listen failed: "+g.cfg.ListenAddr, err)
	}
	g.mu.Lock()
	g.listener = ln
	g.mu.Unlock()
	g.logger.Printf("gateway listening on %s (grace=%s idle=%s policy=%d)",
		ln.Addr(), g.cfg.GracePeriod, g.cfg.IdleTimeout, g.cfg.Quotas.Policy)

	go func() {
		select {
		case <-ctx.Done():
			_ = g.Shutdown(context.Background())
		case <-g.done:
		case <-g.forceCtx.Done():
		}
	}()

	for {
		conn, aerr := ln.Accept()
		if aerr != nil {
			if g.isShuttingDown() || isClosedOrEOF(aerr) {
				return g.waitServeErr()
			}
			// 临时性 Accept 错误可继续，其他错误退出。
			if ne, ok := aerr.(net.Error); ok && ne.Timeout() {
				continue
			}
			err := newError(KindConnIO, "accept", "accept failed", aerr)
			g.markServeErr(err)
			_ = ln.Close()
			g.closeOnce.Do(func() { close(g.done) })
			return err
		}
		g.handleConn(g.forceCtx, conn)
	}
}

func (g *Gateway) markServeErr(err *Error) {
	g.mu.Lock()
	if g.serveErr == nil {
		g.serveErr = err
	}
	g.mu.Unlock()
}

func (g *Gateway) waitServeErr() error {
	g.mu.RLock()
	err := g.serveErr
	g.mu.RUnlock()
	if err != nil {
		return err
	}
	return nil
}

// handleConn 处理一条新连接：配额拒绝、停机竞态与会话注册都在此完成。
func (g *Gateway) handleConn(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()

	// 停机竞态：先检查再申请，停机期间的新接入被明确拒绝并带原因。
	if g.isShuttingDown() {
		g.rejectConn(conn, newError(KindShuttingDown, "accept",
			"gateway shutting down, new connection rejected", nil))
		return
	}

	if g.globalConcurrent != nil {
		if err := g.globalConcurrent.Acquire(ctx, 1); err != nil {
			ge, _ := AsError(err)
			if ge != nil {
				g.logger.Printf("accept rejected remote=%s reason=%s", remote, ge.Kind)
				g.rejectConn(conn, ge)
			} else {
				_ = conn.Close()
			}
			return
		}
	}

	// double-check：申请配额期间可能恰好开始停机。
	if g.isShuttingDown() {
		if g.globalConcurrent != nil {
			g.globalConcurrent.Release(1)
		}
		g.rejectConn(conn, newError(KindShuttingDown, "accept",
			"gateway shutting down after quota acquire", nil))
		return
	}

	seq := sessionSeq.Add(1)
	id := fmt.Sprintf("s-%d-%s", seq, remote)
	sess := newSession(g, id, conn)
	sess.sessionFrames = g.newSessionFramesLimiter()
	sess.sessionBytes = g.newSessionBytesLimiter()

	g.mu.Lock()
	if g.shuttingDown {
		g.mu.Unlock()
		if g.globalConcurrent != nil {
			g.globalConcurrent.Release(1)
		}
		g.rejectConn(conn, newError(KindShuttingDown, "accept",
			"gateway shutting down during registration", nil))
		return
	}
	g.sessions[id] = sess
	g.mu.Unlock()

	g.logger.Printf("session accepted id=%s remote=%s active=%d",
		id, remote, len(g.snapshotIDs()))

	g.sessionWg.Add(1)
	go func() {
		defer g.sessionWg.Done()
		sess.serve(ctx)
	}()
}

// rejectConn 向被拒连接发送 Reject 帧后关闭；发送失败也保证关闭，
// 绝不泄漏连接。
func (g *Gateway) rejectConn(conn net.Conn, reason *Error) {
	_ = g.codec.WriteFrame(conn, &Frame{
		Version: V2,
		Type:    TypeReject,
		Flags:   uint8(reason.Kind),
		Payload: []byte(reason.Kind.String()),
	})
	_ = conn.Close()
}

func (g *Gateway) newSessionFramesLimiter() *Limiter {
	n := g.cfg.Quotas.SessionInflightFrames
	if n <= 0 {
		return nil
	}
	return newLimiter("session-frames", int64(n),
		g.cfg.Quotas.Policy, KindSessionFrames)
}

func (g *Gateway) newSessionBytesLimiter() *Limiter {
	n := g.cfg.Quotas.SessionInflightBytes
	if n <= 0 {
		return nil
	}
	return newLimiter("session-inflight-bytes", n,
		g.cfg.Quotas.Policy, KindSessionBytes)
}

func (g *Gateway) removeSession(s *Session) {
	g.mu.Lock()
	delete(g.sessions, s.id)
	n := len(g.sessions)
	g.mu.Unlock()
	if g.globalConcurrent != nil {
		g.globalConcurrent.Release(1)
	}
	g.logger.Printf("session removed id=%s active=%d", s.id, n)
}

func (g *Gateway) snapshotSessions() []*Session {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*Session, 0, len(g.sessions))
	for _, s := range g.sessions {
		out = append(out, s)
	}
	return out
}

func (g *Gateway) snapshotIDs() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]string, 0, len(g.sessions))
	for id := range g.sessions {
		out = append(out, id)
	}
	return out
}

// ActiveSessions 返回当前活跃会话数。
func (g *Gateway) ActiveSessions() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.sessions)
}

// beginShutdown 幂等标记停机并关闭监听器。
func (g *Gateway) beginShutdown() bool {
	g.mu.Lock()
	if g.shuttingDown {
		g.mu.Unlock()
		return false
	}
	g.shuttingDown = true
	ln := g.listener
	g.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	return true
}

// Shutdown 优雅停机：
//  1. 停止接收新连接（停机期间的接入被明确拒绝）；
//  2. 半关闭所有会话读侧，停止读入新数据；
//  3. 在 GracePeriod 内等待在途帧排空（已接收数据不丢失、不重复）；
//  4. 宽限到期仍未结束的会话被强制终止，原因记为 KindGraceExpired。
//
// 重复调用安全：返回首次停机结果，不会状态回退。
func (g *Gateway) Shutdown(ctx context.Context) error {
	first := g.beginShutdown()
	sessions := g.snapshotSessions()
	g.logger.Printf("shutdown begin graceful=%v sessions=%d", first, len(sessions))

	// 半关闭全部会话读侧。注意：读循环退出后会话可能很快从注册表摘除，
	// 因此排空与最终关闭都基于这里的固定快照，避免漏掉在途帧。
	for _, s := range sessions {
		_ = s.CloseRead(newSessionError(KindShuttingDown, "shutdown", s.id,
			"gateway graceful shutdown: closing read half", nil))
	}

	deadline := time.Now().Add(g.cfg.GracePeriod)
	timedOut := false
	for _, s := range sessions {
		dctx, cancel := context.WithDeadline(ctx, deadline)
		if err := s.drain(dctx); err != nil {
			if ge, ok := AsError(err); ok && ge.Kind == KindGraceExpired {
				timedOut = true
				s.ForceClose(newSessionError(KindGraceExpired, "shutdown", s.id,
					"grace period expired, force terminating session", nil))
			}
		}
		cancel()
		// drain 成功后做正常关闭（幂等）。
		if s.State() != StateClosed {
			_ = s.Close(newSessionError(KindShuttingDown, "shutdown", s.id,
				"graceful close after drain", nil))
		}
	}

	if timedOut {
		// 宽限到期：解除可能仍阻塞在配额申请上的读 goroutine。
		g.triggerForce()
	}

	g.finishServe()
	g.logger.Printf("shutdown complete timedOut=%v remaining=%d",
		timedOut, g.ActiveSessions())
	if timedOut {
		return newError(KindGraceExpired, "shutdown",
			"some sessions were force terminated after grace period", nil)
	}
	return nil
}

// ForceShutdown 立即强制终止全部会话与监听，保留可解释原因。
func (g *Gateway) ForceShutdown(reason string) error {
	g.beginShutdown()
	g.triggerForce()
	if reason == "" {
		reason = "forced shutdown"
	}
	g.logger.Printf("force shutdown: %s sessions=%d", reason, g.ActiveSessions())
	for _, s := range g.snapshotSessions() {
		s.ForceClose(newSessionError(KindForceTerminated, "force-shutdown",
			s.id, reason, nil))
	}
	g.finishServe()
	return newError(KindForceTerminated, "shutdown", reason, nil)
}

// finishServe 关闭监听器并幂等关闭 done，解除 ListenAndServe。
func (g *Gateway) finishServe() {
	g.mu.Lock()
	ln := g.listener
	g.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	g.closeOnce.Do(func() { close(g.done) })
}

// triggerForce 幂等取消 forceCtx，解除所有阻塞在配额申请上的会话读 goroutine。
func (g *Gateway) triggerForce() {
	g.forceOnce.Do(func() { g.forceCancel() })
}

// waitSessions 阻塞直到所有会话 goroutine 退出（主要用于测试/资源回收确认）。
func (g *Gateway) waitSessions(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		g.sessionWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// EchoHandler 是回显处理器：收到什么版本/内容就原样发回，用于本地验证。
type EchoHandler struct{}

func (EchoHandler) ServeFrame(sess *Session, f *Frame) error {
	if f.Type != TypeData {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return sess.Send(ctx, f)
}

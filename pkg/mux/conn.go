package mux

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// inbound 是一个已解码、待分派到具体路的帧。它持有哪些维度的配额用
// 三个 held* 位记录；release* 用 released* CAS 保证每维恰好归还一次。
// 注意 held 与 released 必须分离：held 位置位不能阻止后续 release。
type inbound struct {
	fr     *frame.Frame
	weight int64
	conn   *Conn

	mu sync.Mutex
	st *Stream // 归属路（分派后非 nil）

	// held*：该帧当前是否持有对应维度配额（由分派器在申请成功时置位）。
	heldConn bool
	heldFr   bool
	heldBy   bool

	// released*：每维一次性 CAS，保证归还不重不漏。
	relConn atomicFlag
	relFr   atomicFlag
	relBy   atomicFlag
}

// markHeld 在 mu 下登记路归属与路级持有维度（不改动连接级位）。
func (in *inbound) markHeld(st *Stream, conn, fr, by bool) {
	in.mu.Lock()
	in.st = st
	in.heldConn = in.heldConn || conn
	in.heldFr = fr
	in.heldBy = by
	in.mu.Unlock()
}

// releaseAll 归还该帧持有的全部在途配额（路帧/路字节/连接字节），幂等。
func (in *inbound) releaseAll() {
	in.mu.Lock()
	st := in.st
	hc, hf, hb := in.heldConn, in.heldFr, in.heldBy
	in.mu.Unlock()
	if hb && in.relBy.set() && st != nil {
		st.inBytes.Give(in.weight)
	}
	if hf && in.relFr.set() && st != nil {
		st.inFrames.Give(1)
	}
	if hc && in.relConn.set() {
		in.conn.releaseInboundConn(in.weight)
	}
}

// outItem 是已编码、已占待写配额、排队给写循环的一帧。
type outItem struct {
	buf         []byte
	endStream   bool   // 写完后本端连接级 CloseWrite（StreamID=0 的 END_STREAM）
	endStreamID uint32 // >0 表示写完后该路本地半闭（不做 socket CloseWrite）
	resetID     uint32 // >0 表示 RST_STREAM，无连接级 CloseWrite 语义
}

// Conn 是一条承载多路逻辑流的连接的全部运行时状态。
type Conn struct {
	id      uint64
	conn    net.Conn
	opts    Options
	handler Handler
	logf    func(string, ...any)

	// globalBytes 为网关级全局字节预算；nil 表示不纳入全局记账。
	globalBytes *quota.Limiter

	mu        sync.Mutex
	state     State
	reason    CloseReason
	closeErr  error
	closeOnce sync.Once
	done      chan struct{}

	runCtx    context.Context
	runCancel context.CancelFunc

	// 分派器与读/写循环之间的通道。
	rawCh      chan *inbound    // 读循环 → 分派器
	outCh      chan *outItem    // 所有路 → 写循环
	evCh       chan streamEvent // worker/读循环 → 分派器的唤醒/终结事件
	rstCh      chan *resetReq   // Stream.Reset/Conn 级重置请求 → 分派器
	peerEOF    chan struct{}    // 读循环遇到对端 EOF
	readErr    chan readFault   // 读循环不可恢复故障
	readerDone chan struct{}    // 读循环退出信号

	// stopDisp 在读循环停止后关闭：正常收尾时分派器排空残余 rawCh/事件后退出；
	// 强制终止时由 ctx 取消直接退出。
	stopDisp     chan struct{}
	stopDispOnce sync.Once

	// 仅分派器 goroutine 访问的路由状态。
	streams       map[uint32]*Stream
	seenIDs       map[uint32]struct{} // 曾出现过的 StreamID（回收后用于识别迟到帧）
	active        int
	pendingOpens  []*inbound // 活跃路上限背压：等待“开路”的帧（FIFO）
	pendingOpenID map[uint32]struct{}
	// tableMu 保护 streams 表的“分派器写 / 外部 goroutine 读”交叉访问；
	// active 计数额外用 atomic（见 activeN）。
	tableMu sync.RWMutex
	activeN atomic.Int64

	// 连接级配额。
	connInflight *quota.Limiter
	connOutbound *quota.Limiter

	wgWorkers sync.WaitGroup // 所有路 worker
	wgIO      sync.WaitGroup // 读/写/分派/看门狗

	graceOnce sync.Once
	graceDl   time.Time
	runOnce   sync.Once

	closeSignal chan struct{}
	signalOnce  sync.Once

	// sendMu 串行化“状态检查 + 出站配额 + 入队”，并与 outClosed 配合
	// 杜绝向已关闭通道发送。
	sendMu    sync.Mutex
	outClosed bool

	activity chan struct{}
}

type readFault struct {
	err error
}

// streamEventKind 分派器事件类型。
type streamEventKind uint8

const (
	evKick streamEventKind = iota // worker 处理完一帧，腾出在途槽位
	evEnd                         // 某路进入吸收态，可回收路槽
)

// streamEvent 由 worker（或读循环）发给分派器。
type streamEvent struct {
	kind streamEventKind
	id   uint32
	n    int64 // evKick：本次完成的帧数（通常 1）
}

// resetReq 请求对某一路发送 RST_STREAM 并本地终结。
type resetReq struct {
	id     uint32
	reason CloseReason
	done   chan error
}

// New 基于已建立连接构造多路连接（不启动任何循环）。
func New(id uint64, conn net.Conn, h Handler, opts Options) *Conn {
	if h == nil {
		panic("mux: nil handler")
	}
	if opts.GracePeriod <= 0 {
		opts.GracePeriod = 5 * time.Second
	}
	if opts.StreamQueueFrames <= 0 {
		opts.StreamQueueFrames = 64
	}
	logger := log.New(log.Writer(), fmt.Sprintf("[mux %d] ", id), log.LstdFlags|log.Lmsgprefix)
	runCtx, runCancel := context.WithCancel(context.Background())
	return &Conn{
		id:            id,
		conn:          conn,
		opts:          opts,
		handler:       h,
		logf:          logger.Printf,
		state:         StateActive,
		done:          make(chan struct{}),
		runCtx:        runCtx,
		runCancel:     runCancel,
		rawCh:         make(chan *inbound, 256),
		outCh:         make(chan *outItem, 64),
		evCh:          make(chan streamEvent, 64),
		rstCh:         make(chan *resetReq, 16),
		peerEOF:       make(chan struct{}, 1),
		readErr:       make(chan readFault, 1),
		readerDone:    make(chan struct{}),
		stopDisp:      make(chan struct{}),
		streams:       make(map[uint32]*Stream),
		seenIDs:       make(map[uint32]struct{}),
		pendingOpenID: make(map[uint32]struct{}),
		connInflight:  quota.NewLimiter(opts.MaxConnInflightBytes),
		connOutbound:  quota.NewLimiter(opts.MaxConnOutboundBytes),
		activity:      make(chan struct{}, 1),
		closeSignal:   make(chan struct{}),
	}
}

// SetLogger 注入日志函数（默认输出到 log 标准 logger）。
func (c *Conn) SetLogger(f func(string, ...any)) {
	if f != nil {
		c.logf = f
	}
}

// SetGlobalLimiter 把连接挂到网关级全局字节预算；必须在 Run 前调用。
func (c *Conn) SetGlobalLimiter(l *quota.Limiter) { c.globalBytes = l }

func (c *Conn) ID() uint64 { return c.id }

// Done 在连接彻底终止时关闭。
func (c *Conn) Done() <-chan struct{} { return c.done }

// State 返回连接生命周期状态快照。
func (c *Conn) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Reason 返回连接终止原因。
func (c *Conn) Reason() CloseReason {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// ActiveStreams 返回当前活跃路数快照。
func (c *Conn) ActiveStreams() int {
	return int(c.activeN.Load())
}

// Stream 按 ID 查找路；不存在返回 nil。可从任意 goroutine 调用。
func (c *Conn) Stream(id uint32) *Stream {
	c.tableMu.RLock()
	defer c.tableMu.RUnlock()
	return c.streams[id]
}

// Run 启动读/分派/写/看门狗循环并阻塞到终止；重复调用安全。
func (c *Conn) Run(ctx context.Context) error {
	started := false
	c.runOnce.Do(func() {
		started = true
		c.runLocked(ctx)
	})
	if !started {
		return c.Wait()
	}
	return c.closeErr
}

// Wait 阻塞直到连接彻底终止，返回最终错误（nil=正常）。
func (c *Conn) Wait() error {
	<-c.done
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// CloseWrite 半关闭整条连接的发送侧（StreamID=0 的 END_STREAM + TCP FIN）。
// 各路的独立半关闭请用 Stream.CloseWrite。幂等。
func (c *Conn) CloseWrite() error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	st := c.state
	if st.terminal() || st == StateClosing {
		c.mu.Unlock()
		return ErrConnClosed
	}
	if st == StateLocalHalf {
		c.mu.Unlock()
		return nil
	}
	c.state = StateLocalHalf
	c.mu.Unlock()

	buf, err := frame.Append(nil, &frame.Frame{Version: frame.V2, Flags: frame.FlagEndStream, Stream: 0})
	if err != nil {
		return err
	}
	n := int64(len(buf))
	cctx, cancel := context.WithTimeout(context.Background(), c.opts.GracePeriod)
	defer cancel()
	if err := c.acquireOutbound(cctx, nil, n, false); err != nil {
		return err
	}
	if c.outClosed {
		c.releaseOutbound(nil, n)
		return ErrConnClosed
	}
	select {
	case c.outCh <- &outItem{buf: buf, endStream: true}:
	case <-cctx.Done():
		c.releaseOutbound(nil, n)
		return cctx.Err()
	}
	c.signalActivity()
	return nil
}

// GracefulClose 发起宽限排空；非阻塞，用 Wait/Done 观察结束。幂等。
func (c *Conn) GracefulClose(grace time.Duration) error {
	if grace <= 0 {
		grace = c.opts.GracePeriod
	}
	c.mu.Lock()
	if c.state.terminal() {
		c.mu.Unlock()
		return nil
	}
	c.state = StateClosing
	c.mu.Unlock()
	c.graceOnce.Do(func() {
		c.logf("场景=优雅关闭发起 grace=%s", grace)
		dl := time.Now().Add(grace)
		c.mu.Lock()
		c.graceDl = dl
		c.mu.Unlock()
		// 宽限读截止：先吐出残余完整帧，边界后读循环安静退出。
		_ = c.conn.SetReadDeadline(dl)
	})
	return nil
}

// ForceClose 立即强制终止整条连接与全部路；幂等，保留首次原因。
func (c *Conn) ForceClose(reason CloseReason) error {
	c.forceClose(reason)
	return c.closeErr
}

// forceClose 内部强制终止（幂等）。
func (c *Conn) forceClose(reason CloseReason) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		r := reason
		if r.Code == 0 && r.Text == "" {
			r = ReasonForcedByCaller
		}
		r.Forced = true
		c.state = StateForceTerminated
		c.reason = r
		c.closeErr = fmt.Errorf("mux force-terminated: %s", r)
		c.mu.Unlock()
		c.signalClosing()
		c.logf("场景=强制终止 reason=%s", r)
		c.runCancel()
		_ = c.conn.Close()
	})
}

// drainAndCloseOut 在 sendMu 保护下关闭 outCh，杜绝 send-on-closed。
func (c *Conn) drainAndCloseOut() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if !c.outClosed {
		c.outClosed = true
		close(c.outCh)
	}
}

// finalize 收尾并关闭 done（幂等由 closeOnce 保证状态推进）。
func (c *Conn) finalize(forced bool) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if !forced {
			c.state = StateClosed
			c.reason = ReasonNormal
			c.closeErr = nil
		}
		c.mu.Unlock()
		c.runCancel()
		_ = c.conn.Close()
	})
	// 非强制路径下 forceClose 未触发时也要保证 reason 记录。
	if !forced {
		c.mu.Lock()
		c.state = StateClosed
		if c.reason.Code == 0 && c.reason.Text == "" {
			c.reason = ReasonNormal
		}
		c.mu.Unlock()
	}
	c.signalClosing()
	c.logf("场景=连接收尾 state=%s forced=%v reason=%s", c.State(), forced, c.Reason())
	close(c.done)
}

// Serve 在一条已建立连接上创建并后台运行多路连接。
func Serve(ctx context.Context, id uint64, conn net.Conn, h Handler, opts Options) (*Conn, error) {
	if h == nil {
		return nil, fmt.Errorf("mux: nil handler")
	}
	c := New(id, conn, h, opts)
	go func() { _ = c.Run(ctx) }()
	return c, nil
}

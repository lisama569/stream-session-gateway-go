package mux

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// Handler 处理每一路上的帧；由使用方实现。每一路的所有帧严格串行回调，
// 不同路之间回调可并发。
type Handler interface {
	OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame)
}

// HandlerFunc 函数式 Handler。
type HandlerFunc func(ctx context.Context, st *Stream, f *frame.Frame)

func (fn HandlerFunc) OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame) {
	fn(ctx, st, f)
}

// StreamLifecycle 是可选的流生命周期回调（Handler 可额外实现）。
type StreamLifecycle interface {
	// OnStreamEnd 在某一路进入吸收态后回调；reason 给出收尾/重置原因。
	OnStreamEnd(st *Stream, reason CloseReason)
}

// Stream 是单条逻辑流的运行时状态与句柄。
type Stream struct {
	id   uint32
	conn *Conn

	mu     sync.Mutex
	state  streamState
	reason CloseReason

	ctx    context.Context
	cancel context.CancelFunc

	// inCh：分派器 → 本路 worker，容量 = Options.StreamQueueFrames。
	// 路内严格经此通道串行投递，保证顺序且不串包。
	inCh chan *inbound

	// 路级配额：在途帧/字节（含分派器停放的帧）、待写字节。
	inFrames *quota.Limiter
	inBytes  *quota.Limiter
	outBytes *quota.Limiter

	// 以下字段仅分派器 goroutine 读写：
	// pending 已持有全部配额、但 inCh 暂满而停放的帧（罕见，防御性）。
	// overflow 尚未拿到连接级/路级在途配额、等待配额释放的帧 FIFO。
	// 溢出帧不计入连接在途配额，因此慢路溢出不会占住整条连接的预算。
	pending  []*inbound
	overflow []*inbound
	endSeen  bool // 是否已收到对端 END_STREAM
	sentEnd  bool // 本端是否已排队 END_STREAM
	peer     bool // 对端发起（首帧来自对端）
	opened   bool // 是否已计入连接活跃路数

	// delivered/processed 是分派器视角的“已投递帧数/worker 已处理帧数”，
	// 二者相等且 overflow/pending/inCh 皆空才表示该路帧全部处理完（无在途）。
	// 不能只用 inCh 长度判断，因为最后一帧可能正在 worker 手中。
	// delivered 是分派器视角“已投递帧数”；processed 是分派器从 processedAck
	// 同步过来的“worker 已处理帧数”快照。二者相等且无停放帧才表示全部处理完。
	delivered    int64
	processed    int64
	processedAck atomic.Int64 // worker 每处理完一帧原子递增

	// localSent/remoteSeen 是两个半闭方向位，在 st.mu 下由写循环/分派器更新；
	// state 字段按二者派生对外展示，避免单枚举合并丢失“双方半闭”信息。
	localSent  bool
	remoteSeen bool

	// sendMu 串行化同一路“状态检查+出站配额+入队”，保证 END_STREAM
	// 与其前数据帧不交错，并避免与 outCh 关闭竞态。
	sendMu sync.Mutex

	// closeInOnce 保证 inCh 恰好关闭一次（正常收尾/重置/连接强制）。
	closeInOnce sync.Once
	once        sync.Once // 保证 done 恰好关闭一次
	done        chan struct{}
}

// ID 返回流标记（帧头 StreamID）。
func (st *Stream) ID() uint32 { return st.id }

// Conn 返回所属多路连接。
func (st *Stream) Conn() *Conn { return st.conn }

// State 返回流状态快照。
func (st *Stream) State() streamState {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.state
}

// Reason 返回流终结原因；未终结为零值。
func (st *Stream) Reason() CloseReason {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.reason
}

// Done 在流进入吸收态时关闭。
func (st *Stream) Done() <-chan struct{} { return st.done }

// Context 返回随流终止而取消的上下文；处理器应尊重它以响应强制结束。
func (st *Stream) Context() context.Context { return st.ctx }

// Send 在本路上发送一帧（帧头 StreamID/Flags 以本路与调用入参为准）。
// 同一路多次 Send 经 sendMu 串行化，严格按调用顺序入队，绝不与 END_STREAM
// 或同路其它帧交错；不同路之间互不阻塞。
func (st *Stream) Send(ctx context.Context, f *frame.Frame) error {
	st.sendMu.Lock()
	defer st.sendMu.Unlock()

	st.mu.Lock()
	sst := st.state
	st.mu.Unlock()
	if sst == streamReset {
		return ErrStreamClosed
	}
	if sst == streamLocalHalf || sst == streamClosed {
		return ErrHalfClosed
	}
	c := st.conn
	c.mu.Lock()
	cst := c.state
	c.mu.Unlock()
	if cst.terminal() {
		return ErrConnClosed
	}

	ff := *f
	ff.Stream = st.id
	buf, err := frame.Append(nil, &ff)
	if err != nil {
		return err
	}
	n := int64(len(buf))
	if err := c.acquireOutbound(ctx, st, n, c.opts.InflightPolicy == PolicyReject); err != nil {
		return err
	}
	c.sendMu.Lock()
	if c.outClosed {
		c.sendMu.Unlock()
		c.releaseOutbound(st, n)
		return ErrConnClosed
	}
	select {
	case c.outCh <- &outItem{buf: buf}:
		c.sendMu.Unlock()
		return nil
	case <-ctx.Done():
		c.sendMu.Unlock()
		c.releaseOutbound(st, n)
		return ctx.Err()
	}
}

// CloseWrite 半关闭本路发送方向：排队写出该路 END_STREAM（不做 socket FIN，
// 不影响同连接其它路）。幂等；其后本路 Send 返回 ErrHalfClosed。
func (st *Stream) CloseWrite() error {
	st.sendMu.Lock()
	defer st.sendMu.Unlock()

	st.mu.Lock()
	sst := st.state
	if sst.terminal() {
		st.mu.Unlock()
		if sst == streamReset {
			return ErrStreamClosed
		}
		return nil
	}
	// 用 localSent 做幂等闸门：在排队 END_STREAM 前就置位，避免多个并发
	// CloseWrite 在“帧尚未真正写出、状态尚未推进”的窗口里重复排队 END_STREAM。
	if st.localSent || sst == streamLocalHalf {
		st.mu.Unlock()
		return nil
	}
	st.localSent = true
	st.sentEnd = true
	st.mu.Unlock()

	c := st.conn
	c.mu.Lock()
	cst := c.state
	c.mu.Unlock()
	if cst.terminal() {
		return ErrConnClosed
	}

	buf, err := frame.Append(nil, &frame.Frame{
		Version: frame.V2, Flags: frame.FlagEndStream, Stream: st.id,
	})
	if err != nil {
		return err
	}
	n := int64(len(buf))
	cctx, cancel := context.WithTimeout(context.Background(), c.opts.GracePeriod)
	defer cancel()
	if err := c.acquireOutbound(cctx, st, n, false); err != nil {
		return err
	}
	c.sendMu.Lock()
	if c.outClosed {
		c.sendMu.Unlock()
		c.releaseOutbound(st, n)
		return ErrConnClosed
	}
	select {
	case c.outCh <- &outItem{buf: buf, endStreamID: st.id}:
		c.sendMu.Unlock()
	case <-cctx.Done():
		c.sendMu.Unlock()
		c.releaseOutbound(st, n)
		return cctx.Err()
	}
	c.signalActivity()
	return nil
}

// Reset 立即终止本路并向对端排队发送 RST_STREAM；不影响其它路。
// 幂等，重复调用保留首次原因。
func (st *Stream) Reset(reason CloseReason) error {
	st.mu.Lock()
	if st.state == streamReset {
		st.mu.Unlock()
		return nil
	}
	st.mu.Unlock()
	c := st.conn
	if reason.Code == 0 && reason.Text == "" {
		reason = CloseReason{Code: 20, Text: "local reset", Forced: true}
	}
	req := &resetReq{id: st.id, reason: reason, done: make(chan error, 1)}
	select {
	case c.rstCh <- req:
		select {
		case err := <-req.done:
			return err
		case <-c.runCtx.Done():
			return ErrConnClosed
		}
	default:
		// 分派器已停止（连接收尾中）：本地直接标记，不再强求 RST 发出。
		return ErrConnClosed
	}
}

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/mux"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// 会话级哨兵错误。
var (
	ErrClosed        = errors.New("session: closed")
	ErrHalfClosed    = errors.New("session: write side half-closed")
	ErrQuotaRejected = errors.New("session: quota rejected")
)

// Handler 处理每帧应用逻辑；由使用方实现（单路模式）。
type Handler interface {
	OnFrame(ctx context.Context, s *Session, f *frame.Frame)
}

// HandlerFunc 便于用函数实现 Handler。
type HandlerFunc func(ctx context.Context, s *Session, f *frame.Frame)

func (fn HandlerFunc) OnFrame(ctx context.Context, s *Session, f *frame.Frame) {
	fn(ctx, s, f)
}

// StreamHandler 处理多路模式下某一路的帧；同一流严格串行、不同流并发。
type StreamHandler = mux.Handler

// StreamHandlerFunc 是 StreamHandler 的函数适配器。
type StreamHandlerFunc = mux.HandlerFunc

// Stream 是多路逻辑流句柄的别名，便于从 session 包直接使用。
type Stream = mux.Stream

// MuxStats 多路计数快照别名。
type MuxStats = mux.Stats

// maxProtocolErrors 是连续坏帧（已重同步）的容忍上限，
// 超过则以协议错误终止，避免异常链路无限空转。
const maxProtocolErrors = 8

type outItem struct {
	buf       []byte
	endStream bool // 写成功后本端写侧半关闭（TCP CloseWrite）
	release   func()
}

// dispatched 是已解码、已占入站配额、待执行的处理任务。
type dispatched struct {
	fr     *frame.Frame
	weight int64
}

// Session 封装一条双向流连接的全部运行时状态。
type Session struct {
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

	outCh chan *outItem

	inflightFrames *quota.Limiter
	inflightBytes  *quota.Limiter
	outboundBytes  *quota.Limiter

	// jobCh 把“解码”与“串行处理”解耦：读循环在 handler 卡住时仍能
	// 独立响应宽限读截止/强制终止。无缓冲：串行 worker 忙时读循环背压。
	jobCh chan *dispatched

	handlerWG sync.WaitGroup
	writerWG  sync.WaitGroup

	graceOnce sync.Once
	graceDl   time.Time
	runOnce   sync.Once

	// closeSignal 在状态进入 Closing/吸收态时关闭，唤醒半闭等待。
	closeSignal chan struct{}
	signalOnce  sync.Once

	// sendMu 串行化“状态检查+配额申请+入队”，保证：
	// CloseWrite 的 END_STREAM 不会与其前竞态到达的数据帧乱序；
	// 与 outClosed 配合避免向已关闭通道发送。
	sendMu    sync.Mutex
	outClosed bool

	activity chan struct{}

	// 多路复用（opts.EnableMux 时启用）。
	mx         *mux.Mux
	muxHandler mux.Handler
}

// New 基于已建立的本地连接构造会话（不启动读写循环）。
func New(id uint64, conn net.Conn, h Handler, opts Options) *Session {
	if opts.GracePeriod <= 0 {
		opts.GracePeriod = 5 * time.Second
	}
	logger := log.New(log.Writer(), fmt.Sprintf("[session %d] ", id), log.LstdFlags|log.Lmsgprefix)
	runCtx, runCancel := context.WithCancel(context.Background())
	return &Session{
		runCtx:         runCtx,
		runCancel:      runCancel,
		id:             id,
		conn:           conn,
		opts:           opts,
		handler:        h,
		logf:           logger.Printf,
		state:          StateActive,
		done:           make(chan struct{}),
		outCh:          make(chan *outItem, 64),
		jobCh:          make(chan *dispatched),
		inflightFrames: quota.NewLimiter(int64(opts.MaxInflightFrames)),
		inflightBytes:  quota.NewLimiter(opts.MaxInflightBytes),
		outboundBytes:  quota.NewLimiter(opts.MaxOutboundBytes),
		activity:       make(chan struct{}, 1),
		closeSignal:    make(chan struct{}),
	}
}

// SetGlobalLimiter 把会话挂到网关级全局字节预算上；必须在 Run 前调用。
func (s *Session) SetGlobalLimiter(l *quota.Limiter) { s.globalBytes = l }

// SetStreamHandler 配置多路处理器；必须在 Run 之前调用。
// 调用即等价于为该会话启用多路模式（opts.EnableMux 会被置位）。
func (s *Session) SetStreamHandler(h mux.Handler) {
	s.muxHandler = h
	s.opts.EnableMux = true
}

// Mux 暴露多路复用器（未启用时为 nil）。
func (s *Session) Mux() *mux.Mux { return s.mx }

// setupMux 在 Run 启动前一次性装配多路复用器及其与本会话的接缝。
func (s *Session) setupMux(h mux.Handler) {
	cfg := s.opts.Mux
	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = s.opts.GracePeriod
	}
	policy := mux.PolicyBlock
	if s.opts.InflightPolicy == PolicyReject {
		policy = mux.PolicyReject
	}
	cfg.Policy = policy

	deps := mux.Deps{
		GlobalBytes:        s.globalBytes,
		ConnInflightFrames: s.inflightFrames,
		ConnInflightBytes:  s.inflightBytes,
		ConnOutboundBytes:  s.outboundBytes,
		StreamSlots:        quota.NewGate(cfg.MaxStreams),
		EnqueueOut:         s.enqueueEncoded,
		ConnCloseWrite:     s.CloseWrite,
		ForceConn: func(code uint16, text string) {
			s.forceClose(CloseReason{Code: code, Text: text, Forced: true})
		},
		Logf: s.logf,
	}
	s.mx = mux.New(h, cfg, deps)
	s.mx.Start()
}

// enqueueEncoded 是多路出站接缝：入队一个已编码项，release 在写出或丢弃
// （连接关闭）时恰好调用一次。outCh 关闭/入队被 ctx 取消时返回错误且不持有 release。
func (s *Session) enqueueEncoded(ctx context.Context, buf []byte, endConn bool, release func()) error {
	s.sendMu.Lock()
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	if st.terminal() || st == StateClosing {
		s.sendMu.Unlock()
		return ErrClosed
	}
	if st == StateLocalHalf {
		s.sendMu.Unlock()
		return ErrHalfClosed
	}
	if s.outClosed {
		s.sendMu.Unlock()
		return ErrClosed
	}
	item := &outItem{buf: buf, endStream: endConn, release: release}
	s.sendMu.Unlock()

	select {
	case s.outCh <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MuxStats 返回多路计数快照（未启用时返回零值）。
func (s *Session) MuxStats() mux.Stats {
	if s.mx == nil {
		return mux.Stats{}
	}
	return s.mx.Stats()
}

// ActiveStreams 返回存活逻辑路数（未启用时为 0）。
func (s *Session) ActiveStreams() int {
	if s.mx == nil {
		return 0
	}
	return s.mx.ActiveStreams()
}

func (s *Session) ID() uint64            { return s.id }
func (s *Session) Done() <-chan struct{} { return s.done }

// State 返回当前生命周期状态的快照。
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Reason 返回终止原因；未终止时返回零值。
func (s *Session) Reason() CloseReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason
}

func (s *Session) signalActivity() {
	select {
	case s.activity <- struct{}{}:
	default:
	}
}

// signalClosing 唤醒半闭等待（幂等）。
func (s *Session) signalClosing() {
	s.signalOnce.Do(func() { close(s.closeSignal) })
}

// acquireInbound 按 全局→本会话 的固定顺序申请入站配额，避免跨维度死锁。
func (s *Session) acquireInbound(ctx context.Context, n int64, reject bool) error {
	// 单帧本身超过配额上限：任何时候都无法容纳，立即给出可区分原因。
	if s.inflightFrames.Limit() > 0 && 1 > s.inflightFrames.Limit() {
		return quota.NewRejected(quota.ReasonSessionInflightFrames, s.inflightFrames.Limit(), 1)
	}
	if s.inflightBytes.Limit() > 0 && n > s.inflightBytes.Limit() {
		return quota.NewRejected(quota.ReasonSessionInflightBytes, s.inflightBytes.Limit(), n)
	}
	if s.globalBytes != nil && s.globalBytes.Limit() > 0 && n > s.globalBytes.Limit() {
		return quota.NewRejected(quota.ReasonGlobalInflightBytes, s.globalBytes.Limit(), n)
	}
	take := func(l *quota.Limiter, w int64, r quota.Reason) error {
		if l == nil || l.Limit() <= 0 {
			return nil
		}
		if reject {
			if l.TryTake(w) {
				return nil
			}
			return quota.NewRejected(r, l.Limit(), w)
		}
		return l.Take(ctx, w)
	}
	if err := take(s.globalBytes, n, quota.ReasonGlobalInflightBytes); err != nil {
		return err
	}
	if err := take(s.inflightFrames, 1, quota.ReasonSessionInflightFrames); err != nil {
		if s.globalBytes != nil {
			s.globalBytes.Give(n)
		}
		return err
	}
	if err := take(s.inflightBytes, n, quota.ReasonSessionInflightBytes); err != nil {
		s.inflightFrames.Give(1)
		if s.globalBytes != nil {
			s.globalBytes.Give(n)
		}
		return err
	}
	return nil
}

func (s *Session) releaseInbound(n int64) {
	s.inflightBytes.Give(n)
	s.inflightFrames.Give(1)
	if s.globalBytes != nil {
		s.globalBytes.Give(n)
	}
}

// acquireOutbound / releaseOutbound 维护待写字节的全局+本会话配额。
func (s *Session) acquireOutbound(ctx context.Context, n int64, reject bool) error {
	take := func(l *quota.Limiter, w int64, r quota.Reason) error {
		if l == nil || l.Limit() <= 0 {
			return nil
		}
		if w > l.Limit() {
			return quota.NewRejected(r, l.Limit(), w)
		}
		if reject {
			if l.TryTake(w) {
				return nil
			}
			return quota.NewRejected(r, l.Limit(), w)
		}
		return l.Take(ctx, w)
	}
	if err := take(s.globalBytes, n, quota.ReasonGlobalInflightBytes); err != nil {
		return err
	}
	if err := take(s.outboundBytes, n, quota.ReasonSessionOutboundBytes); err != nil {
		if s.globalBytes != nil {
			s.globalBytes.Give(n)
		}
		return err
	}
	return nil
}

func (s *Session) releaseOutbound(n int64) {
	s.outboundBytes.Give(n)
	if s.globalBytes != nil {
		s.globalBytes.Give(n)
	}
}

// Send 入队一帧等待写出（语义见 README）。
// 排空（Closing）期间允许在途处理器继续把结果写出——这正是“已接收数据的
// 处理不丢失”的保证；但排空开始后新到达的数据不会再被读循环派发。
func (s *Session) Send(ctx context.Context, f *frame.Frame) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	if st.terminal() {
		return ErrClosed
	}
	if st == StateLocalHalf {
		return ErrHalfClosed
	}
	buf, err := frame.Append(nil, f)
	if err != nil {
		return err
	}
	n := int64(len(buf))
	if err := s.acquireOutbound(ctx, n, s.opts.InflightPolicy == PolicyReject); err != nil {
		return fmt.Errorf("%w: %w", ErrQuotaRejected, err)
	}
	if s.outClosed {
		s.releaseOutbound(n)
		return ErrClosed
	}
	select {
	case s.outCh <- &outItem{buf: buf}:
		return nil
	case <-ctx.Done():
		s.releaseOutbound(n)
		return ctx.Err()
	}
}

// CloseWrite 半关闭发送方向。幂等。
// 若对端也已半关闭，则进入优雅排空（双向完整关闭）。
func (s *Session) CloseWrite() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.mu.Lock()
	st := s.state
	if st.terminal() || st == StateClosing {
		s.mu.Unlock()
		return ErrClosed
	}
	if st == StateLocalHalf {
		s.mu.Unlock()
		return nil
	}
	wasRemote := st == StateRemoteHalf
	s.state = StateLocalHalf
	s.mu.Unlock()
	buf, err := frame.Append(nil, &frame.Frame{Version: frame.V2, Flags: frame.FlagEndStream})
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(context.Background(), s.opts.GracePeriod)
	defer cancel()
	if err := s.acquireOutbound(cctx, int64(len(buf)), false); err != nil {
		return err
	}
	select {
	case s.outCh <- &outItem{buf: buf, endStream: true}:
	case <-cctx.Done():
		s.releaseOutbound(int64(len(buf)))
		return cctx.Err()
	}
	s.signalActivity()
	if wasRemote {
		return s.GracefulClose(s.opts.GracePeriod)
	}
	return nil
}

// GracefulClose 发起宽限排空；非阻塞返回，实际结束用 Wait/Done 观察。
// 语义：停止接收新帧；在途处理器与待写帧在 grace 内排空则正常关闭，
// 否则以 ReasonGraceExpired 强制终止。重复调用幂等。
func (s *Session) GracefulClose(grace time.Duration) error {
	if grace <= 0 {
		grace = s.opts.GracePeriod
	}
	s.mu.Lock()
	if s.state.terminal() {
		s.mu.Unlock()
		return nil
	}
	if s.state == StateLocalHalf {
		// 已半关闭发送侧时仍允许排空，状态推进为 Closing。
	}
	s.state = StateClosing
	s.mu.Unlock()
	s.graceOnce.Do(func() {
		s.logf("场景=优雅关闭发起 grace=%s", grace)
		// 宽限期内读循环继续接收并处理“对端已发出、尚在途中”的帧，
		// 不截断、不丢失；读截止设为宽限边界：边界后读循环安静退出
		//（i/o timeout 在 Closing 下被识别为“排空读截止”而非链路故障）。
		// 在途处理器若在边界仍未排空，则由 runLocked 强制终止兜底。
		dl := time.Now().Add(grace)
		s.mu.Lock()
		s.graceDl = dl
		s.mu.Unlock()
		_ = s.conn.SetReadDeadline(dl)
	})
	return nil
}

// ForceClose 立即强制终止并保留原因；幂等，重复调用保留首次原因。
func (s *Session) ForceClose(reason CloseReason) error {
	s.forceClose(reason)
	return s.closeErr
}

func (s *Session) forceClose(reason CloseReason) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		r := reason
		if !r.Forced {
			r.Forced = true
		}
		if r.Code == 0 && r.Text == "" {
			r = ReasonForcedByCaller
		}
		s.state = StateForceTerminated
		s.reason = r
		s.closeErr = fmt.Errorf("session force-terminated: %s", r)
		s.mu.Unlock()
		s.signalClosing()
		s.logf("场景=强制终止 reason=%s", r)
		s.runCancel()
		_ = s.conn.Close() // 解除所有 Read/Write 阻塞
	})
}

// Wait 阻塞直到会话彻底终止，返回最终错误（nil=正常结束）。
func (s *Session) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// Run 启动读/写/看门狗循环并阻塞直到彻底终止。重复调用安全：
// 首个调用执行生命周期，后续调用等待并返回同一个最终错误。
func (s *Session) Run(ctx context.Context) error {
	started := false
	s.runOnce.Do(func() {
		started = true
		s.runLocked(ctx)
	})
	if !started {
		return s.Wait()
	}
	return s.closeErr
}

func (s *Session) runLocked(parent context.Context) {
	ctx, cancel := s.runCtx, s.runCancel
	// 父 context 取消联动到会话内部上下文。
	stopParent := make(chan struct{})
	go func() {
		select {
		case <-parent.Done():
			cancel()
		case <-stopParent:
		}
	}()
	defer close(stopParent)

	muxMode := s.opts.EnableMux && s.muxHandler != nil
	if muxMode {
		s.setupMux(s.muxHandler)
	}

	s.writerWG.Add(1)
	go s.writeLoop(ctx)
	if !muxMode {
		if !s.opts.ConcurrentHandlers {
			s.handlerWG.Add(1)
			go s.serialWorker(ctx)
		}
	}
	if s.opts.IdleTimeout > 0 {
		go s.idleWatchdog(ctx)
	}

	readAbort := make(chan struct{})
	readOutcome := s.readLoop(ctx, readAbort)
	close(readAbort)

	// 读循环退出后判定终结路径。
	s.mu.Lock()
	forced := s.state == StateForceTerminated
	s.mu.Unlock()

	if forced {
		// 强制终止：立即解除整个会话。关闭 jobCh 阻止新任务入队，
		// 出站队列立即关闭丢弃待写项。
		close(s.jobCh)
		if muxMode {
			s.mx.AbortAll("session force-terminated")
		}
		s.drainAndCloseOut(true)
		s.writerWG.Wait()
		s.finalize(readOutcome, true)
		return
	}

	// 优雅/正常路径：先判断是否处于 GracefulClose 排空。
	s.mu.Lock()
	closing := s.state == StateClosing
	graceDl := s.graceDl
	s.mu.Unlock()

	if muxMode {
		s.drainMuxMode(closing, graceDl, readOutcome)
		return
	}

	if closing {
		// 关闭 jobCh：串行 worker 会把当前帧处理完再退出（jobCh 无缓冲，
		// 不存在“排队但永不处理”的任务）；用剩余宽限等它排空，
		// 到期仍未完成则强制终止（ReasonGraceExpired）。
		close(s.jobCh)
		remaining := time.Until(graceDl)
		handlersDone := make(chan struct{})
		go func() { s.handlerWG.Wait(); close(handlersDone) }()
		select {
		case <-handlersDone:
		case <-time.After(remaining + 10*time.Millisecond):
			s.forceClose(ReasonGraceExpired)
			s.drainAndCloseOut(true)
			s.writerWG.Wait()
			s.finalize(readOutcome, true)
			return
		}
	} else {
		// 普通路径（对端 FIN/END_STREAM）：关闭 jobCh 让串行 worker 排空
		// 当前帧后退出；并发模式下等待所有 goroutine 完成。保证“已接收
		// 数据”全部处理完才进入关闭。
		close(s.jobCh)
		s.handlerWG.Wait()
	}
	s.drainAndCloseOut(false)
	s.writerWG.Wait()
	s.finalize(readOutcome, false)
}

// drainAndCloseOut 在 sendMu 保护下标记并关闭出站队列，
// 杜绝与并发 Send 的“send on closed channel”竞态。
// 关闭后写循环独占消费剩余项：正常路径写出，连接已坏时统一归还配额
// （不会重复归还，因为 Send 看到 outClosed 后不再入队）。
func (s *Session) drainAndCloseOut(discard bool) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if !s.outClosed {
		s.outClosed = true
		close(s.outCh)
	}
}

// drainMuxMode 处理多路模式读循环退出后的收尾：
//   - GracefulClose 排空：BeginDrain → 等到 graceDl；期间在途路继续处理并可
//     Send 结果；到期仍卡住的路被强制终止，整条连接以 ReasonGraceExpired
//     （原因文本含卡住流 ID）终止，停机不被一路挂死。
//   - 对端 EOF/END_STREAM 正常路径：关闭输入，让各路把已收帧处理完即收尾。
func (s *Session) drainMuxMode(closing bool, graceDl time.Time, readOutcome readResult) {
	if closing {
		s.mx.BeginDrain(graceDl)
		stuck := s.mx.WaitDrain(graceDl)
		if len(stuck) > 0 {
			r := ReasonGraceExpired
			r.Text = fmt.Sprintf("grace drain expired, stuck streams=%v", stuck)
			s.forceClose(r)
			s.drainAndCloseOut(true)
			s.writerWG.Wait()
			s.finalize(readOutcome, true)
			return
		}
		s.drainAndCloseOut(false)
		s.writerWG.Wait()
		s.finalize(readOutcome, false)
		return
	}
	// 对端正常结束（EOF/控制流半闭后的 FIN）：停止接新帧，各路排空已收数据。
	s.mx.BeginDrain(time.Now().Add(s.opts.GracePeriod))
	dl := time.Now().Add(s.opts.GracePeriod)
	stuck := s.mx.WaitDrain(dl)
	if len(stuck) > 0 {
		r := CloseReason{Code: 3, Text: fmt.Sprintf("peer closed but streams stuck=%v", stuck), Forced: true}
		s.forceClose(r)
		s.drainAndCloseOut(true)
		s.writerWG.Wait()
		s.finalize(readOutcome, true)
		return
	}
	s.drainAndCloseOut(false)
	s.writerWG.Wait()
	s.finalize(readOutcome, false)
}

func (s *Session) finalize(readOutcome readResult, forced bool) {
	cancel := s.runCancel
	s.closeOnce.Do(func() {
		s.mu.Lock()
		if !forced {
			reason := ReasonNormal
			if readOutcome.peerEOF {
				reason = ReasonPeerEndStream
			}
			s.state = StateClosed
			s.reason = reason
			s.closeErr = nil
			s.mu.Unlock()
			s.logf("场景=正常关闭 reason=%s", reason)
		} else {
			s.mu.Unlock()
		}
		cancel()
		_ = s.conn.Close()
	})
	close(s.done)
}

type readResult struct {
	err     error
	peerEOF bool
}

func (s *Session) readLoop(ctx context.Context, aborted <-chan struct{}) readResult {
	dec := frame.NewDecoder(s.conn)
	var f frame.Frame
	badStreak := 0
	outcome := readResult{}
	muxMode := s.mx != nil
	for {
		s.mu.Lock()
		st := s.state
		s.mu.Unlock()
		if st.terminal() {
			return outcome
		}
		// Closing 时不立即返回：GracefulClose 已把读截止设为“现在”，
		// Next 会先吐出残余缓冲中已凑齐的完整帧（不丢在途数据），
		// 下一次 Read 立即超时，由 default 分支识别并退出。
		err := dec.Next(&f)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				outcome.peerEOF = true
				if muxMode {
					// 多路模式：EOF 表示对端关闭整个写方向，停止接新帧，
					// 已收帧交由各路排空；不做单路会话的半关闭等待。
					s.mx.BeginDrain(time.Now())
				}
				return outcome
			case errors.Is(err, io.ErrUnexpectedEOF):
				outcome.err = err
				s.forceClose(CloseReason{Code: 11, Text: "truncated frame: " + err.Error(), Forced: true})
				return outcome
			case errors.Is(err, frame.ErrUnknownVersion),
				errors.Is(err, frame.ErrLengthExceeded),
				errors.Is(err, frame.ErrChecksum):
				badStreak++
				s.logf("场景=坏帧已重同步 连续=%d/%d 错误=%v", badStreak, maxProtocolErrors, err)
				if badStreak >= maxProtocolErrors {
					s.forceClose(CloseReason{Code: 11, Text: "too many corrupt frames: " + err.Error(), Forced: true})
				}
				f = frame.Frame{}
				continue
			default:
				if ctx.Err() != nil || s.isForced() {
					return outcome // 强制终止/取消导致的 I/O 错误，不是链路故障
				}
				// 优雅排空期间的读截止到期：读侧到此为止，交由正常排空收尾，
				// 不记为链路故障（是否强制由 graceCoordinator 依据排空结果决定）。
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() && s.isClosing() {
					s.logf("场景=宽限读截止到达，停止接收 错误=%v", err)
					if muxMode {
						s.mx.BeginDrain(s.graceDl)
					}
					return outcome
				}
				outcome.err = err
				s.forceClose(CloseReason{Code: 12, Text: "read failure: " + err.Error(), Forced: true})
				return outcome
			}
		}
		badStreak = 0
		s.signalActivity()

		if s.opts.NegotiatedVersion != 0 && byte(f.Version) != s.opts.NegotiatedVersion {
			s.forceClose(CloseReason{Code: 11,
				Text: fmt.Sprintf("version mismatch: got %d want %d", f.Version, s.opts.NegotiatedVersion), Forced: true})
			return outcome
		}

		if muxMode {
			connHalf, derr := s.dispatchMuxFrame(ctx, &f)
			if derr != nil {
				return outcome
			}
			f = frame.Frame{}
			if connHalf {
				s.onPeerEndStream()
				outcome.peerEOF = true
				s.waitAfterPeerHalf(ctx)
				if s.isForced() {
					outcome.peerEOF = false
				}
				return outcome
			}
			continue
		}

		fr := f
		f = frame.Frame{}
		n := int64(len(fr.Payload))
		qerr := s.acquireInbound(ctx, n, s.opts.InflightPolicy == PolicyReject)
		if qerr != nil {
			s.logf("场景=入站配额终止 错误=%v", qerr)
			s.forceClose(CloseReason{Code: 13, Text: qerr.Error(), Forced: true})
			return outcome
		}
		end := fr.EndStream()
		s.dispatchFrame(ctx, &dispatched{fr: &fr, weight: n}, aborted)

		if end {
			// 对端半关闭：等待本端动作（CloseWrite/双向完成 → GracefulClose
			// 会设置读截止；ForceClose 关 conn；或对端彻底关闭 → EOF）。
			s.onPeerEndStream()
			outcome.peerEOF = true
			s.waitAfterPeerHalf(ctx)
			if s.isForced() {
				outcome.peerEOF = false
			}
			return outcome
		}
	}
}

// waitAfterPeerHalf 在收到对端 END_STREAM 后阻塞，直到：
//   - GracefulClose（closeSignal / 读截止到期）；
//   - ForceClose（ctx 取消、conn 关闭）；
//   - 对端彻底关闭连接（后台探测到 EOF）。
func (s *Session) waitAfterPeerHalf(ctx context.Context) {
	connClosed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, s.conn)
		close(connClosed)
	}()
	for {
		s.mu.Lock()
		st := s.state
		s.mu.Unlock()
		if st.terminal() || st == StateClosing {
			return
		}
		select {
		case <-s.closeSignal:
			return
		case <-ctx.Done():
			return
		case <-connClosed:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// dispatchFrame 把已占配额的帧交给执行模型：
//   - 串行（默认）：投递给唯一的 serialWorker，严格保序不串包；
//   - 并发：每帧独立 goroutine。
//
// 投递失败（会话被强制终止、读循环已退出）时必须释放本帧配额，
// 避免记账泄漏；用 select+aborted 保证绝不向“不会再被消费”的通道阻塞。
func (s *Session) dispatchFrame(ctx context.Context, job *dispatched, aborted <-chan struct{}) {
	if s.opts.ConcurrentHandlers {
		s.handlerWG.Add(1)
		go func(j *dispatched) {
			defer s.handlerWG.Done()
			defer s.releaseInbound(j.weight)
			s.handler.OnFrame(ctx, s, j.fr)
		}(job)
		return
	}
	select {
	case s.jobCh <- job:
	case <-aborted:
		s.releaseInbound(job.weight)
	case <-ctx.Done():
		s.releaseInbound(job.weight)
	}
}

// serialWorker 是串行模式唯一的处理 goroutine，保证接收顺序即处理顺序。
func (s *Session) serialWorker(ctx context.Context) {
	defer s.handlerWG.Done()
	for job := range s.jobCh {
		func() {
			defer s.releaseInbound(job.weight)
			s.handler.OnFrame(ctx, s, job.fr)
		}()
	}
}

// dispatchMuxFrame 把一帧交给多路复用器，并把“连接级”拒绝翻译为连接终止；
// 路级拒绝由 mux 自行丢帧/终止该路，读循环继续处理后续其它路的好帧。
func (s *Session) dispatchMuxFrame(ctx context.Context, f *frame.Frame) (bool, error) {
	connHalf, err := s.mx.Deliver(ctx, f)
	if err == nil {
		return connHalf, nil
	}
	if rj, ok := mux.AsReject(err); ok {
		if rj.Scope == mux.ScopeStream {
			// 理论上不可达：流级拒收已在 mux 内部吸收；防御性继续读后续帧。
			s.logf("场景=路级入站拒收未吸收 stream=%d 原因=%s", rj.StreamID, rj.Error())
			return false, nil
		}
		// 连接级/全局级收不下：终止整条连接，原因可区分级与方向。
		s.logf("场景=连接级入站配额终止 错误=%v", rj)
		s.forceClose(CloseReason{Code: 13, Text: rj.Error(), Forced: true})
		return false, err
	}
	if ctx.Err() != nil || s.isForced() {
		return false, nil
	}
	s.forceClose(CloseReason{Code: 13, Text: err.Error(), Forced: true})
	return false, err
}

func (s *Session) isForced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == StateForceTerminated
}

func (s *Session) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == StateClosing
}

func (s *Session) onPeerEndStream() {
	s.mu.Lock()
	st := s.state
	if st.terminal() || st == StateClosing {
		s.mu.Unlock()
		return
	}
	wasLocal := st == StateLocalHalf
	// 本端尚未半闭：推进为 RemoteHalf；本端已半闭：保留 LocalHalf，
	// 由 CloseWrite 的 wasRemote 路径负责发起排空。
	if !wasLocal {
		s.state = StateRemoteHalf
	}
	s.mu.Unlock()
	s.logf("场景=收到对端END_STREAM state=%s", s.State())
	if wasLocal {
		_ = s.GracefulClose(s.opts.GracePeriod)
	}
}

// releaseOutItem 归还一个已写出/已丢弃出站项占用的配额：
// 多路项（带 release）由 mux 释放 全局/连接/路 三级；旧单路项由会话释放。
// 恰好调用一次，杜绝重复归还。
func (s *Session) releaseOutItem(item *outItem) {
	if item.release != nil {
		item.release()
		return
	}
	s.releaseOutbound(int64(len(item.buf)))
}

// writeLoop 串行写出，保证帧不交错；outCh 关闭后排空剩余帧再退出。
func (s *Session) writeLoop(ctx context.Context) {
	defer s.writerWG.Done()
	var halfClosed bool
	for item := range s.outCh {
		if halfClosed {
			// 理论不可达：CloseWrite 后 Send 被拒；防御性归还。
			s.releaseOutItem(item)
			continue
		}
		if s.opts.WriteTimeout > 0 {
			_ = s.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
		}
		_, werr := s.conn.Write(item.buf)
		s.releaseOutItem(item)
		if werr != nil {
			if !s.isForced() && ctx.Err() == nil {
				s.forceClose(CloseReason{Code: 12, Text: "write failure: " + werr.Error(), Forced: true})
			}
			// 排空并归还后续排队项（连接已不可写）。
			for rest := range s.outCh {
				s.releaseOutItem(rest)
			}
			return
		}
		s.signalActivity()
		if item.endStream {
			if cw, ok := s.conn.(interface{ CloseWrite() error }); ok {
				_ = cw.CloseWrite()
			}
			halfClosed = true
		}
	}
}

// idleWatchdog 在任意方向静默达到 IdleTimeout 时强制终止（原因可解释）。
func (s *Session) idleWatchdog(ctx context.Context) {
	t := time.NewTimer(s.opts.IdleTimeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-s.activity:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(s.opts.IdleTimeout)
		case <-t.C:
			s.forceClose(ReasonIdleTimeout)
			return
		}
	}
}

// Serve 在一条已建立连接上创建并运行会话直到终止（阻塞）。
// gateway 用它在 accept goroutine 中调用；如需后台运行请自行 go Serve。
func Serve(ctx context.Context, id uint64, conn net.Conn, h Handler, opts Options) (*Session, error) {
	if h == nil {
		return nil, errors.New("session: nil handler")
	}
	s := New(id, conn, h, opts)
	go func() { _ = s.Run(ctx) }()
	return s, nil
}

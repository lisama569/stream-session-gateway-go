package mux

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// defaultQueueCap 是“未配置在途帧上限”时每路接收队列的硬安全容量，
// 保证即便全部是 0 字节空帧，内存也不会随流量无界增长；队列满时读循环背压。
const defaultQueueCap = 1024

// job 是已解码、已占入站配额、待该路 worker 串行处理的一个任务。
type job struct {
	fr     *frame.Frame
	weight int64
	once   sync.Once
}

// Stream 是一路逻辑流的运行时与应用句柄。
//
// 不变量：
//   - 投递（jobCh 发送）只可能来自连接读循环这一个 goroutine；
//   - 消费（worker）每路唯一，保证路内严格保序、不串包；
//   - 正常输入结束由读循环关闭 jobCh（唯一发送方关闭，安全）；
//   - 异常终止关闭 stopCh，绝不关闭 jobCh，杜绝“send on closed”。
type Stream struct {
	id      uint32
	control bool // stream 0：连接级控制流，不占活跃路数配额
	m       *Mux

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	state     StreamState
	reason    string
	writeHalf bool // 本端是否已发出 END_STREAM（localHalf 及之后）

	jobCh  chan *job
	stopCh chan struct{} // 异常终止信号（Abort/连接强制终止）
	done   chan struct{} // 进入吸收态后关闭

	workerWG sync.WaitGroup // 仅含该路 worker，排空判定用

	inFrames *quota.Limiter
	inBytes  *quota.Limiter
	outBytes *quota.Limiter

	// currentJob 指向 worker 正在执行的任务（atomic.Pointer），供连接级
	// 强制终止时立即代还其配额；处理器之后返回时 releaseOnce 成为空操作。
	currentJob atomic.Pointer[job]

	closeJobOnce sync.Once
	stopOnce     sync.Once
	doneOnce     sync.Once
}

func newStream(m *Mux, id uint32, control bool) *Stream {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := m.cfg
	capFrames := cfg.MaxStreamInflightFrames
	chCap := capFrames
	if chCap <= 0 {
		chCap = defaultQueueCap
	}
	return &Stream{
		id:       id,
		control:  control,
		m:        m,
		ctx:      ctx,
		cancel:   cancel,
		state:    StreamActive,
		jobCh:    make(chan *job, chCap),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		inFrames: quota.NewLimiter(int64(cfg.MaxStreamInflightFrames)),
		inBytes:  quota.NewLimiter(cfg.MaxStreamInflightBytes),
		outBytes: quota.NewLimiter(cfg.MaxStreamOutboundBytes),
	}
}

// ID 返回流 ID。
func (s *Stream) ID() uint32 { return s.id }

// Done 在该路进入吸收态（Closed/Aborted）后关闭。
func (s *Stream) Done() <-chan struct{} { return s.done }

// State 返回状态快照。
func (s *Stream) State() StreamState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Reason 返回异常终止原因；正常/未终止为空串。
func (s *Stream) Reason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason
}

func (s *Stream) isTerminalLocked() bool { return s.state == StreamClosed || s.state == StreamAborted }

// aborted 在异常路径判定。
func (s *Stream) aborted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == StreamAborted
}

// snapshot 在锁外组装可观测快照。
func (s *Stream) snapshot() StreamInfo {
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	return StreamInfo{
		ID:       s.id,
		State:    st,
		Inflight: s.inBytes.Used(),
		Outbound: s.outBytes.Used(),
	}
}

// markRemoteHalf 处理对端 END_STREAM：仅推进状态（帧本身照常派给处理器）。
// 返回推进后的状态。
func (s *Stream) markRemoteHalf() StreamState {
	s.mu.Lock()
	switch s.state {
	case StreamActive:
		s.state = StreamRemoteHalf
	case StreamLocalHalf:
		s.state = StreamBothHalf
	}
	st := s.state
	s.mu.Unlock()
	s.m.emit(s.id, EventRemoteHalf, "state="+st.String())
	return st
}

// Send 在该路上排队一帧，保证与同路其它 Send/CloseWrite 的相对顺序。
func (s *Stream) Send(ctx context.Context, f *frame.Frame) error {
	if f == nil {
		f = &frame.Frame{}
	}
	// 流归属不可伪造：以句柄所属流为准写帧头。
	ff := *f
	ff.Stream = s.id

	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	if st == StreamAborted || st == StreamClosed {
		return ErrStreamClosed
	}
	if st == StreamLocalHalf || st == StreamBothHalf {
		return ErrStreamHalfClosed
	}

	buf, err := frame.Append(nil, &ff)
	if err != nil {
		return err
	}
	n := int64(len(buf))
	if err := s.acquireOutbound(ctx, n); err != nil {
		return err
	}
	// 拿到配额后再确认一次状态，避免终止窗口里入队。
	s.mu.Lock()
	st = s.state
	s.mu.Unlock()
	if st == StreamAborted || st == StreamClosed {
		s.releaseOutbound(n)
		return ErrStreamClosed
	}
	if st == StreamLocalHalf || st == StreamBothHalf {
		s.releaseOutbound(n)
		return ErrStreamHalfClosed
	}

	release := func() { s.releaseOutbound(n) }
	if err := s.m.deps.EnqueueOut(ctx, buf, false, release); err != nil {
		// 入队失败（ctx 取消/连接已关）：EnqueueOut 不会持有 release，这里归还。
		s.releaseOutbound(n)
		return err
	}
	return nil
}

// CloseWrite 半关闭该路写侧：排队一个该流 END_STREAM 帧。幂等。
// 控制流（id=0）半关闭整个连接（TCP FIN 语义）。
func (s *Stream) CloseWrite(ctx context.Context) error {
	if s.control {
		return s.m.deps.ConnCloseWrite()
	}
	s.mu.Lock()
	st := s.state
	if st.terminal() {
		s.mu.Unlock()
		return ErrStreamClosed
	}
	if st == StreamLocalHalf || st == StreamBothHalf {
		s.mu.Unlock()
		return nil
	}
	wasRemote := st == StreamRemoteHalf
	if wasRemote {
		s.state = StreamBothHalf
	} else {
		s.state = StreamLocalHalf
	}
	s.writeHalf = true
	s.mu.Unlock()

	buf, err := frame.Append(nil, &frame.Frame{Version: frame.V2, Flags: frame.FlagEndStream, Stream: s.id})
	if err != nil {
		return err
	}
	n := int64(len(buf))
	if err := s.acquireOutbound(ctx, n); err != nil {
		return err
	}
	release := func() { s.releaseOutbound(n) }
	if err := s.m.deps.EnqueueOut(ctx, buf, false, release); err != nil {
		s.releaseOutbound(n)
		return err
	}
	s.m.emit(s.id, EventLocalHalf, "state="+s.State().String())
	// 对端也已半闭且本路在途已排空时由 worker 收尾；若 worker 早已退出，
	// 在此立即尝试收尾。
	if wasRemote {
		s.tryFinishAfterWorker()
	}
	return nil
}

// Abort 强制终止该路（不影响其它路）。幂等，保留首次原因。
func (s *Stream) Abort(reason string) { s.abort(reason) }

func (s *Stream) abort(reason string) bool {
	s.mu.Lock()
	if s.isTerminalLocked() {
		s.mu.Unlock()
		return false
	}
	s.state = StreamAborted
	if reason == "" {
		reason = "aborted"
	}
	s.reason = reason
	s.mu.Unlock()

	s.stopOnce.Do(func() { close(s.stopCh) })
	s.cancel()
	s.closeDone()
	if !s.control {
		s.m.slotGateGive()
		atomic.AddInt64(&s.m.stats.Aborted, 1)
		s.m.retireStream(s)
	}
	s.m.emit(s.id, EventAborted, "reason="+reason)
	s.m.logf("场景=逻辑流异常终止 stream=%d reason=%s", s.id, reason)
	return true
}

// closeDone 幂等关闭吸收信号。
func (s *Stream) closeDone() {
	s.doneOnce.Do(func() { close(s.done) })
}

// acquireOutbound 按 全局→连接→本路 固定顺序申请待写配额。
func (s *Stream) acquireOutbound(ctx context.Context, n int64) error {
	d := s.m.deps
	take := func(l *quota.Limiter, w int64, r quota.Reason, scope Scope) error {
		if l == nil || l.Limit() <= 0 {
			return nil
		}
		if w > l.Limit() {
			return &RejectError{Scope: scope, Dir: DirWrite, Reason: r, StreamID: s.id, Limit: l.Limit(), Asked: w}
		}
		if s.m.cfg.Policy == PolicyReject {
			if l.TryTake(w) {
				return nil
			}
			return &RejectError{Scope: scope, Dir: DirWrite, Reason: r, StreamID: s.id, Limit: l.Limit(), Asked: w}
		}
		if err := l.Take(ctx, w); err != nil {
			return err
		}
		return nil
	}
	if err := take(d.GlobalBytes, n, quota.ReasonGlobalInflightBytes, ScopeGlobal); err != nil {
		return err
	}
	if err := take(d.ConnOutboundBytes, n, quota.ReasonSessionOutboundBytes, ScopeConn); err != nil {
		s.giveQuiet(d.GlobalBytes, n)
		return err
	}
	if err := take(s.outBytes, n, quota.ReasonStreamOutboundBytes, ScopeStream); err != nil {
		s.giveQuiet(d.ConnOutboundBytes, n)
		s.giveQuiet(d.GlobalBytes, n)
		return err
	}
	return nil
}

func (s *Stream) releaseOutbound(n int64) {
	s.giveQuiet(s.outBytes, n)
	s.giveQuiet(s.m.deps.ConnOutboundBytes, n)
	s.giveQuiet(s.m.deps.GlobalBytes, n)
}

// giveQuiet 在限流器为 nil/无限时 Give 本就是空操作，统一收口。
func (s *Stream) giveQuiet(l *quota.Limiter, n int64) {
	if l != nil {
		l.Give(n)
	}
}

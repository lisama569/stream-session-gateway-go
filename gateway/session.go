package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// State 是会话生命周期状态。
type State int

const (
	StateActive   State = iota // 双向正常
	StateDraining              // 读侧已半关闭，排空在途帧，写侧仍可用
	StateClosing               // 正在关闭（发送/等待关闭完成）
	StateClosed                // 终态
)

func (s State) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateDraining:
		return "draining"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Handler 处理入站帧。返回错误会导致会话以该错误关闭。
type Handler interface {
	ServeFrame(sess *Session, f *Frame) error
}

// HandlerFunc 让普通函数满足 Handler。
type HandlerFunc func(sess *Session, f *Frame) error

func (h HandlerFunc) ServeFrame(sess *Session, f *Frame) error { return h(sess, f) }

// Session 表示一条双向流会话。
type Session struct {
	gw    *Gateway
	id    string
	conn  net.Conn
	codec *Codec

	writeMu sync.Mutex // 串行化对 conn 的写

	mu       sync.Mutex
	state    State
	closeErr *Error // 首次关闭原因，终态后不再改变
	done     chan struct{}
	readHalf bool // 读侧是否已半关闭

	processing    sync.WaitGroup // 在途（已接收未处理完）帧计数
	sessionFrames *Limiter       // 本会话在途帧数配额
	sessionBytes  *Limiter       // 本会话在途字节数配额

	closedOnce sync.Once
}

func newSession(gw *Gateway, id string, conn net.Conn) *Session {
	return &Session{
		gw:    gw,
		id:    id,
		conn:  conn,
		codec: gw.codec,
		state: StateActive,
		done:  make(chan struct{}),
	}
}

// Context 返回随会话终结（关闭/强制终止）而取消的上下文。
// 业务 Handler 应在长时间工作中 select 它，以便在停机时及时退出。
func (s *Session) Context() context.Context { return s.gw.forceCtx }

// ID 返回会话标识。
func (s *Session) ID() string { return s.id }

// State 返回当前生命周期状态。
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *Session) logf(format string, args ...any) {
	s.gw.logger.Printf("session=%s state=%s: "+format,
		append([]any{s.id, s.State()}, args...)...)
}

// isClosed 必须在持锁状态下调用。
func (s *Session) isClosedLocked() bool { return s.state == StateClosed }

// Send 线程安全地向对端发送一帧。
// 读侧半关闭（Draining）时仍允许写；进入终态后返回 KindSessionClosed。
func (s *Session) Send(ctx context.Context, f *Frame) error {
	s.mu.Lock()
	if s.isClosedLocked() {
		err := newSessionError(KindSessionClosed, "send", s.id,
			"send on closed session", s.closeErr)
		s.mu.Unlock()
		return err
	}
	// Draining（读半关闭）期间仍允许写，保证在途帧的回写不丢失。
	// 读侧半关闭后不会再派发新帧，故无需在此检查网关停机标记。
	s.mu.Unlock()

	buf, err := s.codec.Encode(f)
	if err != nil {
		return err
	}

	// 尊重 ctx 的写截止时间，避免停机时无限阻塞在写上。
	if dl, ok := ctx.Deadline(); ok {
		_ = s.conn.SetWriteDeadline(dl)
	} else {
		_ = s.conn.SetWriteDeadline(time.Time{})
	}

	s.writeMu.Lock()
	_, werr := s.conn.Write(buf)
	s.writeMu.Unlock()

	if werr != nil {
		return newSessionError(KindConnIO, "send", s.id,
			"failed to write frame", werr)
	}
	return nil
}

// CloseRead 半关闭读侧：停止读入并进入 Draining，写侧继续可用。
// 重复调用幂等。
func (s *Session) CloseRead(reason *Error) error {
	s.mu.Lock()
	if s.readHalf {
		s.mu.Unlock()
		return nil
	}
	s.readHalf = true
	wasClosed := s.isClosedLocked()
	if s.state == StateActive {
		s.state = StateDraining
	}
	s.mu.Unlock()

	if rc, ok := s.conn.(interface{ CloseRead() error }); ok {
		_ = rc.CloseRead()
	}
	if reason == nil {
		reason = newSessionError(KindShuttingDown, "close-read", s.id,
			"read half closed for drain", nil)
	}
	s.logf("read half closed: %s", reason.Message)
	if wasClosed {
		return nil
	}
	return nil
}

// Close 幂等的正常关闭：尽力通知对端、进入终态并关闭连接。
// 重复调用返回首次关闭原因（不报错、不崩溃、状态不回退）。
func (s *Session) Close(reason *Error) error {
	return s.finish(reason, false)
}

// ForceClose 立即强制终止并保留可解释的原因。
func (s *Session) ForceClose(reason *Error) {
	_ = s.finish(reason, true)
}

// finish 统一的关闭路径。force=true 时不等待、不发送关闭帧。
func (s *Session) finish(reason *Error, force bool) error {
	s.mu.Lock()
	// Closed：已经终结；Closing：另一个 finish 正在执行。两者都直接返回，
	// 保证重复关闭/并发关闭不重复执行关闭流程、状态不回退。
	if s.state == StateClosed || s.state == StateClosing {
		first := s.closeErr
		s.mu.Unlock()
		if first != nil {
			return first
		}
		return nil
	}
	if reason == nil {
		reason = newSessionError(KindSessionClosed, "close", s.id,
			"session closed", nil)
	}
	// 强制终止的可解释原因优先覆盖一个"无原因"的记录位，
	// 但不覆盖已经存在的明确原因。
	s.closeErr = reason
	s.state = StateClosing
	s.readHalf = true
	s.mu.Unlock()

	if !force {
		// 尽力发送 TypeClose（Flags 携带错误类别），失败也不影响关闭。
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = s.Send(ctx, &Frame{Version: V2, Type: TypeClose,
			Flags: uint8(reason.Kind), Seq: 0, Payload: []byte(reason.Kind.String())})
		cancel()
	}

	_ = s.conn.Close()

	s.mu.Lock()
	s.state = StateClosed
	s.mu.Unlock()
	s.closedOnce.Do(func() { close(s.done) })
	s.logf("closed force=%v reason=%s", force, reason.Message)
	return reason
}

// Done 在会话进入终态时关闭。
func (s *Session) Done() <-chan struct{} { return s.done }

// Err 返回关闭原因；存活期间为 nil。
func (s *Session) Err() *Error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// drain 等待在途帧处理完成或 ctx 结束。
func (s *Session) drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.processing.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return newSessionError(KindGraceExpired, "drain", s.id,
			"grace period expired while draining inflight frames", ctx.Err())
	}
}

// serve 驱动读循环直到连接结束、空闲超时或被关闭。
func (s *Session) serve(ctx context.Context) {
	var serveErr *Error
	defer func() {
		s.mu.Lock()
		alreadyClosed := s.isClosedLocked()
		hc := s.readHalf
		s.mu.Unlock()
		switch {
		case alreadyClosed:
			// 已被 Shutdown/ForceShutdown 终结，无需重复关闭。
		case serveErr != nil:
			// 空闲超时 / IO 错误：强制终结并保留可解释原因。
			s.ForceClose(serveErr)
		case hc:
			// 读侧已半关闭而写侧仍活：不在此关闭连接，
			// 由网关 Shutdown 在排空后做最终关闭。
		default:
			// 对端正常断开（EOF）：正常关闭。
			_ = s.Close(nil)
		}
		s.gw.removeSession(s)
	}()

	dec := NewDecoder(s.codec)
	buf := make([]byte, s.gw.cfg.ReadBuffer)
	if s.gw.cfg.IdleTimeout > 0 {
		_ = s.conn.SetReadDeadline(time.Now().Add(s.gw.cfg.IdleTimeout))
	}

	for {
		if ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		readClosed := s.readHalf
		closed := s.isClosedLocked()
		s.mu.Unlock()
		if readClosed || closed {
			return
		}

		n, rerr := s.conn.Read(buf)
		if n > 0 {
			if s.gw.cfg.IdleTimeout > 0 {
				_ = s.conn.SetReadDeadline(time.Now().Add(s.gw.cfg.IdleTimeout))
			}
			frames, derrs := dec.Feed(buf[:n])
			for _, de := range derrs {
				de.SessionID = s.id
				s.logf("decode error recoverable: kind=%s msg=%s buffered=%d",
					de.Kind, de.Message, dec.Buffered())
			}
			for _, f := range frames {
				if stop := s.dispatch(ctx, f); stop {
					return
				}
			}
		}
		if rerr != nil {
			if isClosedOrEOF(rerr) {
				serveErr = nil
			} else if isTimeout(rerr) {
				serveErr = newSessionError(KindIdleTimeout, "read", s.id,
					"idle timeout, no frame received within deadline", rerr)
			} else {
				serveErr = newSessionError(KindConnIO, "read", s.id,
					"connection read failure", rerr)
			}
			return
		}
	}
}

// dispatch 处理单帧：申请三级配额后把业务处理派发到独立 goroutine。
//
// 派发而不是同步处理是会话级配额生效的前提：同一会话允许多帧在途，
// 其并发数与字节数被 SessionInflightFrames/Bytes 严格约束（内存有界）。
// 配额申请本身在 PolicyBlock 下阻塞，形成对 TCP 读侧的背压；
// PolicyReject 下立即回送带可区分原因的 Reject 帧。
// 返回 true 表示会话应停止读循环。
func (s *Session) dispatch(ctx context.Context, f *Frame) (stop bool) {
	if f.Type == TypeClose {
		s.logf("received close frame from peer kind=%s", f.Flags)
		return true
	}

	payloadBytes := int64(len(f.Payload))
	if payloadBytes == 0 {
		payloadBytes = 1
	}

	// 先登记“在途”，再申请配额。这样在 PolicyBlock 阻塞等待配额期间，
	// 该帧也被优雅停机的 drain 计数覆盖，不会出现“配额已申请但尚未登记”
	// 导致 drain 提前结束、在途帧丢失的窗口。
	s.processing.Add(1)
	if err := s.acquireInflight(ctx, payloadBytes); err != nil {
		s.processing.Done()
		if ge, ok := AsError(err); ok {
			s.logf("frame rejected seq=%d reason=%s", f.Seq, ge.Kind)
			_ = s.sendReject(f, ge)
			return false
		}
		// ctx 取消（如停机）：停止循环，交由关闭流程处理。
		return true
	}

	go func() {
		defer s.processing.Done()
		defer s.releaseInflight(payloadBytes)
		s.processFrame(f)
	}()
	return false
}

// processFrame 执行业务 Handler；panic 被恢复为日志，
// 防止单个会话的业务缺陷拖垮整个网关。
func (s *Session) processFrame(f *Frame) {
	defer func() {
		if r := recover(); r != nil {
			s.logf("handler panic recovered seq=%d: %v", f.Seq, r)
		}
	}()
	if err := s.gw.cfg.Handler.ServeFrame(s, f); err != nil {
		if ge, ok := AsError(err); ok {
			s.logf("handler rejected seq=%d kind=%s: %s", f.Seq, ge.Kind, ge.Message)
			_ = s.sendReject(f, ge)
			return
		}
		s.logf("handler error seq=%d: %v", f.Seq, err)
	}
}

// acquireInflight 依次申请三级配额；任一失败则回滚已申请部分。
func (s *Session) acquireInflight(ctx context.Context, bytes int64) error {
	limiters := []*Limiter{
		s.sessionFrames,
		s.sessionBytes,
		s.gw.globalBytes,
	}
	acquired := 0
	for _, l := range limiters {
		if l == nil {
			continue
		}
		n := int64(1)
		if l == s.sessionBytes || l == s.gw.globalBytes {
			n = bytes
		}
		if err := l.Acquire(ctx, n); err != nil {
			for j := acquired - 1; j >= 0; j-- {
				l2 := limiters[j]
				n2 := int64(1)
				if l2 == s.sessionBytes || l2 == s.gw.globalBytes {
					n2 = bytes
				}
				l2.Release(n2)
			}
			if ge, ok := AsError(err); ok {
				ge.SessionID = s.id
				return ge
			}
			return err
		}
		acquired++
	}
	return nil
}

func (s *Session) releaseInflight(bytes int64) {
	if l := s.sessionFrames; l != nil {
		l.Release(1)
	}
	if l := s.sessionBytes; l != nil {
		l.Release(bytes)
	}
	if l := s.gw.globalBytes; l != nil {
		l.Release(bytes)
	}
}

// sendReject 向对端回送配额/处理拒绝回执，Flags 携带可区分的错误类别。
func (s *Session) sendReject(f *Frame, ge *Error) error {
	rctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.Send(rctx, &Frame{
		Version: V2,
		Type:    TypeReject,
		Flags:   uint8(ge.Kind),
		Seq:     f.Seq,
		Payload: []byte(ge.Kind.String()),
	})
}

func isClosedOrEOF(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// sessionSeq 用于生成会话 ID 的进程内计数器。
var sessionSeq atomic.Uint64

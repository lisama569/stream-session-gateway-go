package mux

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
)

// TestStreamLevelQuotaReject：拒收策略下，某一路在途帧配额被打满后，
// 该路被终止（aborted），后续帧被丢弃且不影响其它路；拒绝原因可区分
// 路级/连接级、收/写。
func TestStreamLevelQuotaReject(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	var once int32

	var otherDone int64
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if st.ID() == 1 {
			if atomic.CompareAndSwapInt32(&once, 0, 1) {
				close(started)
			}
			<-release
			return
		}
		atomic.AddInt64(&otherDone, 1)
	})

	cfg := DefaultConfig()
	cfg.Policy = PolicyReject
	cfg.MaxStreams = 8
	cfg.MaxStreamInflightFrames = 3 // stream 1 在途上限 3 帧
	m, env := newTestMux(t, h, cfg)

	// 占 1 个 worker 槽，再排队 2 帧 => 在途=3 达上限；第 4 帧触发路级拒收。
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 0, false)); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 1, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 2, false)); err != nil {
		t.Fatal(err)
	}
	// 第 4 帧：路级收不下。Deliver 内部吸收为“终止该路 + 丢帧”，
	// 不返回连接级错误（连接必须继续）。
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 3, false)); err != nil {
		t.Fatalf("stream-level reject must not surface conn error, got %v", err)
	}
	// 路级拒收后该路立即异常终止并从存活表摘除（槽位归还）。
	if !waitFor(time.Second, func() bool {
		_, exists := m.StreamInfo(1)
		return !exists && m.Stats().Aborted >= 1
	}) {
		t.Fatalf("stream 1 must be aborted and retired, exists=%v stats=%+v",
			func() bool { _, ok := m.StreamInfo(1); return ok }(), m.Stats())
	}
	if !env.hasEvent(1, EventAborted) {
		t.Fatal("aborted event missing for stream 1")
	}
	if m.Stats().RejectedStreams < 1 {
		t.Fatalf("RejectedStreams=%d want >=1", m.Stats().RejectedStreams)
	}
	if len(env.forceTexts()) != 0 {
		t.Fatalf("connection must not be force-closed by stream-level reject: %v", env.forceTexts())
	}

	// 其它路照常工作。
	if _, err := m.Deliver(context.Background(), payloadFrame(2, 0, false)); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return atomic.LoadInt64(&otherDone) == 1 }) {
		t.Fatal("other stream must still process")
	}
	t.Logf("场景=路级配额拒收 stream=1状态=%s 拒收帧=%d 判定依据=该路aborted且连接未终止,stream=2仍处理=%d",
		StreamAborted, m.Stats().RejectedStreams, atomic.LoadInt64(&otherDone))
}

// TestRejectErrorDimensions 直接构造拒绝错误，校验级/方向/原因可被 As 提取，
// errors.Is(ErrQuotaRejected) 成立。
func TestRejectErrorDimensions(t *testing.T) {
	cases := []struct {
		name   string
		scope  Scope
		dir    Direction
		isConn bool
	}{
		{"stream-recv", ScopeStream, DirRecv, false},
		{"stream-write", ScopeStream, DirWrite, false},
		{"conn-recv", ScopeConn, DirRecv, true},
		{"conn-write", ScopeConn, DirWrite, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error = &RejectError{Scope: c.scope, Dir: c.dir, StreamID: 7, Limit: 10, Asked: 11}
			if !errors.Is(err, ErrQuotaRejected) {
				t.Fatal("must wrap ErrQuotaRejected")
			}
			rj, ok := AsReject(err)
			if !ok || rj.Scope != c.scope || rj.Dir != c.dir || rj.StreamID != 7 {
				t.Fatalf("as reject = %+v ok=%v", rj, ok)
			}
			if c.isConn != (rj.Scope == ScopeConn) {
				t.Fatalf("connection-scope mismatch")
			}
		})
	}
}

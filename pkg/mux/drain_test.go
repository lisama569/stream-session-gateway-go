package mux

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
)

// TestGraceExpiryAbortsStuckStream：宽限排空中，一路及时收尾，另一路处理器
// 一直卡住；到点后卡住的路必须被强制终止并出现在返回的 stuck 列表中，
// 停机不被挂死，且该路占用的在途配额被归还。
func TestGraceExpiryAbortsStuckStream(t *testing.T) {
	releaseStuck := make(chan struct{})
	defer close(releaseStuck)
	stuckInWorker := make(chan struct{})
	var stuckOnce int32

	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if st.ID() == 9 {
			if stuckOnce == 0 {
				stuckOnce++
				close(stuckInWorker)
			}
			// 无礼处理器：忽略 ctx，一直不结束。
			<-releaseStuck
			return
		}
		// 好路：回一个 END_STREAM 正常收尾。
		_ = st.CloseWrite(ctx)
	})
	cfg := DefaultConfig()
	m, _ := newTestMux(t, h, cfg)

	// 卡住路：发普通帧占住 worker（不 END，队列里再压 2 帧占在途配额）。
	if _, err := m.Deliver(context.Background(), payloadFrame(9, 0, false)); err != nil {
		t.Fatal(err)
	}
	<-stuckInWorker
	if _, err := m.Deliver(context.Background(), payloadFrame(9, 1, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Deliver(context.Background(), payloadFrame(9, 2, false)); err != nil {
		t.Fatal(err)
	}

	// 好路：END_STREAM + 处理器回 END_STREAM，双向半闭后正常收尾。
	if _, err := m.Deliver(context.Background(), payloadFrame(3, 0, true)); err != nil {
		t.Fatal(err)
	}
	if !waitFor(2*time.Second, func() bool { return m.Stats().ClosedNormally >= 1 }) {
		t.Fatal("good stream did not finish during drain window")
	}

	dl := time.Now().Add(120 * time.Millisecond)
	m.BeginDrain(dl)
	stuck := m.WaitDrain(dl)

	if len(stuck) != 1 || stuck[0] != 9 {
		t.Fatalf("stuck=%v want [9]", stuck)
	}
	// abort 后流即从存活表摘除（槽位归还）；done 已关闭、Stats 记录异常终止。
	if _, exists := m.StreamInfo(9); exists {
		t.Fatal("aborted stream 9 must be retired from live table")
	}
	if m.Stats().Aborted < 1 {
		t.Fatalf("Aborted=%d want >=1", m.Stats().Aborted)
	}
	if m.ActiveStreams() != 0 {
		t.Fatalf("active=%d want 0 after grace expiry", m.ActiveStreams())
	}
	t.Logf("场景=宽限到期一路卡住 stuck=%v 好路已收尾=%d 活跃=%d 判定依据=到点卡路被abort且不拖死停机",
		stuck, m.Stats().ClosedNormally, m.ActiveStreams())
}

// TestAbortAllReleasesEverything：连接级强制终止立即释放全部在途配额，
// 忽略 ctx 的卡住处理器不阻塞终止，存活表清空。
func TestAbortAllReleasesEverything(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	var once int32
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if atomic.CompareAndSwapInt32(&once, 0, 1) {
			close(started)
		}
		<-release // 无礼处理器：忽略 ctx
	})
	cfg := DefaultConfig()
	cfg.MaxStreams = 4
	e := newTestEnv()
	d := e.deps(cfg, testLimits{connFrames: 100, connBytes: 1 << 20})
	m := New(h, cfg, d)
	m.Start()

	if _, err := m.Deliver(context.Background(), payloadFrame(1, 0, false)); err != nil {
		t.Fatal(err)
	}
	<-started
	for seq := 1; seq <= 3; seq++ {
		if _, err := m.Deliver(context.Background(), payloadFrame(1, seq, false)); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(time.Second, func() bool { return d.ConnInflightBytes.Used() > 0 }) {
		t.Fatal("expected inbound quota to be held")
	}
	held := d.ConnInflightBytes.Used()

	doneAbort := make(chan struct{})
	go func() { m.AbortAll("connection down"); close(doneAbort) }()
	select {
	case <-doneAbort:
	case <-time.After(2 * time.Second):
		t.Fatal("AbortAll must not wait for a handler ignoring ctx")
	}
	if d.ConnInflightBytes.Used() != 0 || d.ConnInflightFrames.Used() != 0 {
		t.Fatalf("quota not released: bytes=%d frames=%d (held bytes=%d)",
			d.ConnInflightBytes.Used(), d.ConnInflightFrames.Used(), held)
	}
	if m.ActiveStreams() != 0 {
		t.Fatalf("active=%d want 0", m.ActiveStreams())
	}
	t.Logf("场景=连接强制终止 释放持有字节=%d 判定依据=忽略ctx处理器不阻塞且各级在途记账归零", held)
}

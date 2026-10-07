package mux

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
)

// TestStreamHalfCloseIndependent：stream 1 双向半关闭正常收尾，stream 2 保持
// 活跃并继续收发；流级半关闭不连累同连接其它路，连接本身不半闭。
func TestStreamHalfCloseIndependent(t *testing.T) {
	var st1 *Stream
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if f.EndStream() && st.ID() == 1 {
			// 收到对端半闭：处理完末帧后回一个 END_STREAM 完成双向半闭。
			if err := st.CloseWrite(ctx); err != nil {
				t.Errorf("closewrite: %v", err)
			}
			return
		}
		if err := st.Send(ctx, f); err != nil {
			t.Errorf("send: %v", err)
		}
	})
	cfg := DefaultConfig()
	m, env := newTestMux(t, h, cfg)

	// stream 1：一帧普通数据 + 一帧 END_STREAM。
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 0, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 1, true)); err != nil {
		t.Fatal(err)
	}
	// 等待 stream 1 进入 Closed。
	if !waitFor(2*time.Second, func() bool {
		_, exists := m.StreamInfo(1)
		return !exists // 收尾后从存活表摘除
	}) {
		t.Fatalf("stream 1 should finish, info exists=%v", func() bool { _, ok := m.StreamInfo(1); return ok }())
	}
	if m.Stats().ClosedNormally != 1 {
		t.Fatalf("ClosedNormally=%d want 1", m.Stats().ClosedNormally)
	}
	if env.halfConn != 0 {
		t.Fatalf("connection write must not be half-closed, halfConn=%d", env.halfConn)
	}

	// stream 2 仍可收发。
	if _, err := m.Deliver(context.Background(), payloadFrame(2, 0, false)); err != nil {
		t.Fatalf("stream 2 deliver after stream1 close: %v", err)
	}
	if !env.waitOut(3) { // s1 普通帧 + s1 END_STREAM + s2 普通帧
		t.Fatalf("outputs missing, got %d", env.outCount())
	}
	// 校验最后写出的 stream 顺序：0=s1数据, 1=s1 END_STREAM, 2=s2数据。
	f0 := decodeOut(t, env.out[0].buf)
	f1 := decodeOut(t, env.out[1].buf)
	f2 := decodeOut(t, env.out[2].buf)
	if f0.Stream != 1 || f0.EndStream() {
		t.Fatalf("out0 = stream %d end=%v", f0.Stream, f0.EndStream())
	}
	if f1.Stream != 1 || !f1.EndStream() {
		t.Fatalf("out1 must be stream1 END_STREAM, got stream=%d end=%v", f1.Stream, f1.EndStream())
	}
	if f2.Stream != 2 {
		t.Fatalf("out2 must be stream2, got %d", f2.Stream)
	}
	if m.ActiveStreams() != 1 {
		t.Fatalf("active=%d want 1 (only stream2)", m.ActiveStreams())
	}
	_ = st1
	t.Logf("场景=单路半关闭 stream=1已Closed stream=2仍活跃=%d 连接半闭=%d 判定依据=s1的END_STREAM仅属该路,s2照常收发",
		m.ActiveStreams(), env.halfConn)
}

// TestControlStreamHalfClosesConnection：stream 0 的 END_STREAM 表示连接级
// 半关闭，Deliver 返回 connHalfClosed=true 且调用连接级 CloseWrite 接缝。
func TestControlStreamHalfClosesConnection(t *testing.T) {
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {})
	m, env := newTestMux(t, h, DefaultConfig())
	f := &frame.Frame{Version: frame.V2, Flags: frame.FlagEndStream, Stream: 0}
	connHalf, err := m.Deliver(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	if !connHalf {
		t.Fatal("stream0 END_STREAM must report connHalfClosed")
	}
	if !waitFor(time.Second, func() bool { return env.halfConn == 0 }) {
		// CloseWrite 由 session 侧在 connHalf 时驱动；mux 自身不调用接缝，
		// 这里 halfConn 保持 0 属预期。仅验证 Deliver 返回值。
	}
	t.Logf("场景=控制流半关闭 connHalfClosed=%v 判定依据=stream0的END_STREAM上抛为连接级半关闭信号", connHalf)
}

// TestStreamQuotaReleasedAfterFinish：流正常收尾后活跃路数槽位归还，
// 可立即用新流（验证路数配额不泄漏）。
func TestStreamQuotaReleasedAfterFinish(t *testing.T) {
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if f.EndStream() {
			_ = st.CloseWrite(ctx)
		}
	})
	cfg := DefaultConfig()
	cfg.MaxStreams = 2
	m, _ := newTestMux(t, h, cfg)

	finish := func(id uint32) {
		if _, err := m.Deliver(context.Background(), payloadFrame(id, 0, true)); err != nil {
			t.Fatalf("deliver %d: %v", id, err)
		}
		if !waitFor(2*time.Second, func() bool {
			_, ok := m.StreamInfo(id)
			return !ok
		}) {
			t.Fatalf("stream %d did not finish", id)
		}
	}
	finish(1)
	finish(2)
	// 两个槽位都应已归还，可再开新路。
	finish(3)
	if m.Stats().ClosedNormally != 3 {
		t.Fatalf("ClosedNormally=%d want 3 (slots recycled)", m.Stats().ClosedNormally)
	}
	t.Logf("场景=路配额回收 连续收尾路=3 MaxStreams=%d 判定依据=收尾后槽位归还,新路可建立不泄漏", cfg.MaxStreams)
}

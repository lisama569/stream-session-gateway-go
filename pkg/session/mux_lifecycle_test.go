package session

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/mux"
)

// TestMuxSlowStreamRealBackpressure：stream 1 处理器一直卡住并把自己的在途
// 队列与读循环顶到背压；stream 2 在慢路卡住前已入队的帧仍被独立 worker
// 处理并回写，证明跨路互不阻塞。
func TestMuxSlowStreamRealBackpressure(t *testing.T) {
	releaseSlow := make(chan struct{})
	defer close(releaseSlow)
	slowStarted := make(chan struct{})
	var slowOnce int32
	var fastEcho int64

	h := StreamHandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if st.ID() == 1 {
			if slowOnce == 0 {
				slowOnce = 1
				close(slowStarted)
			}
			select {
			case <-releaseSlow:
			case <-ctx.Done():
			}
			return
		}
		if err := st.Send(ctx, f); err == nil {
			atomic.AddInt64(&fastEcho, 1)
		}
	})
	opts := muxEchoOpts()
	opts.Mux.MaxStreamInflightFrames = 4
	p := newMuxTCPPair(t, opts, h)
	defer p.client.Close()
	defer p.s.ForceClose(ReasonForcedByCaller)

	// 慢路第一帧占住 worker。
	writeClientFrame(t, p.client, &frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("hold")})
	<-slowStarted
	time.Sleep(50 * time.Millisecond)

	// 快路 3 帧（此时慢路 worker 卡住但其队列未满，读循环还能读快路帧）。
	for i := 0; i < 3; i++ {
		writeClientFrame(t, p.client, &frame.Frame{Version: frame.V2, Stream: 2, Payload: []byte("fast")})
	}

	// 客户端读取快路回显（设读超时防止挂死）。
	dec := frame.NewDecoder(p.client)
	gotFast := 0
	deadline := time.Now().Add(3 * time.Second)
	for gotFast < 3 {
		_ = p.client.SetReadDeadline(deadline)
		var gf frame.Frame
		if err := dec.Next(&gf); err != nil {
			t.Fatalf("fast stream echo blocked by slow stream: %v (got=%d)", err, gotFast)
		}
		if gf.Stream == 2 {
			gotFast++
		}
	}
	if atomic.LoadInt64(&fastEcho) != 3 {
		t.Fatalf("fast handler ran %d want 3", atomic.LoadInt64(&fastEcho))
	}
	log.Printf("场景=真实连接跨路不阻塞 slow=1卡住 fast=2回显=%d 判定依据=慢路占worker时快路3帧全部按时回写", gotFast)
}

// TestMuxGraceShutdownStuckStream：网关式停机（GracefulClose）宽限到期时，
// 一路处理器一直不结束，会话必须以 grace-expired 原因强制终止，Wait 返回
// 非 nil 错误且原因可解释，停机不被该路挂死。
func TestMuxGraceShutdownStuckStream(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	var once int32
	h := StreamHandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if once == 0 {
			once = 1
			close(started)
		}
		// 无礼处理器：忽略 ctx，模拟一路卡死。
		<-release
	})
	opts := muxEchoOpts()
	p := newMuxTCPPair(t, opts, h)
	defer p.client.Close()

	writeClientFrame(t, p.client, &frame.Frame{Version: frame.V2, Stream: 5, Payload: []byte("stuck")})
	<-started

	grace := 150 * time.Millisecond
	start := time.Now()
	if err := p.s.GracefulClose(grace); err != nil {
		t.Fatal(err)
	}
	err := p.s.Wait()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("grace expiry with stuck stream must return non-nil error")
	}
	if p.s.State() != StateForceTerminated {
		t.Fatalf("state=%s want force_terminated", p.s.State())
	}
	if p.s.Reason().Code != ReasonGraceExpired.Code {
		t.Fatalf("reason=%s want grace-expired code=%d", p.s.Reason(), ReasonGraceExpired.Code)
	}
	if elapsed > grace+2*time.Second {
		t.Fatalf("shutdown hung for %s by a stuck stream", elapsed)
	}
	if p.s.ActiveStreams() != 0 {
		t.Fatalf("active streams=%d want 0", p.s.ActiveStreams())
	}
	log.Printf("场景=多路停机宽限到期一路卡住 state=%s reason=%q 耗时=%s 判定依据=grace到点强制终止且不被挂死, 活跃=%d",
		p.s.State(), p.s.Reason(), elapsed.Round(time.Millisecond), p.s.ActiveStreams())
}

var _ = mux.DefaultConfig

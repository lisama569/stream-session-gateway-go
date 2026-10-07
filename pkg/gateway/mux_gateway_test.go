package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/mux"
)

// TestMuxGatewayShutdownOneStuck 网关停机：一条多路连接上一路永久卡住，
// 其它路正常；宽限到点网关以 gateway-down(code=4) 强制终止，停机不被挂死。
func TestMuxGatewayShutdownOneStuck(t *testing.T) {
	release := make(chan struct{})
	opts := mux.DefaultOptions()
	opts.GracePeriod = 5 * time.Second // 默认宽限；网关 Shutdown 会显式传 120ms 覆盖

	h := mux.HandlerFunc(func(ctx context.Context, st *mux.Stream, f *frame.Frame) {
		if st.ID() == 42 {
			select {
			case <-release:
			case <-ctx.Done(): // 尊重 ctx，强制终止后退出，不泄漏 goroutine
			}
			return
		}
		_ = st.Send(ctx, f)
	})

	cfg := Config{
		Listen:         "127.0.0.1:0",
		MaxConnections: 10,
		Mux:            opts,
		ShutdownGrace:  120 * time.Millisecond,
	}
	g := NewMux(h, cfg)
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer close(release)

	conn, err := net.Dial("tcp", g.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 卡住一路，另两路正常 echo。
	send := func(f *frame.Frame) {
		if _, err := frame.Encode(conn, f); err != nil {
			t.Logf("encode: %v", err)
		}
	}
	send(&frame.Frame{Version: frame.V2, Stream: 42, Payload: []byte("stuck")})
	send(&frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("ok1")})

	var endCode int32 = -1
	var endMu sync.Mutex
	g.cfg.OnConnEnd = func(id uint64, st EndState) {
		endMu.Lock()
		endCode = int32(st.Code)
		endMu.Unlock()
	}

	dec := frame.NewDecoder(conn)
	var gf frame.Frame
	gotEcho := make(chan error, 1)
	go func() { gotEcho <- dec.Next(&gf) }()
	select {
	case err := <-gotEcho:
		if err != nil || gf.Stream != 1 || string(gf.Payload) != "ok1" {
			t.Fatalf("正常路 echo 异常: %v %q", err, gf.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("停机前正常路未完成")
	}

	// 停机：宽限到期，卡住路让 Shutdown 走强制路径；必须在合理时间内返回。
	done := make(chan error, 1)
	go func() { done <- g.Shutdown(120 * time.Millisecond) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("停机被卡住的一路挂死（宽限强制未生效）")
	}
	if g.ActiveSessions() != 0 {
		t.Fatalf("停机后活跃会话应为0，got %d", g.ActiveSessions())
	}
	endMu.Lock()
	code := endCode
	endMu.Unlock()
	// 网关 Shutdown 把 120ms 宽限显式传给 mux.GracefulClose，因此卡住路由
	// mux 自己在宽限到点以 grace-expired(code=3) 强制终止（优先于网关 100ms
	// 后的 code=4 兜底）。两种原因都表示“到点真正终止卡住的路、不停机挂死”。
	if code != 3 && code != 4 {
		t.Fatalf("卡住路应被强制终止，原因应为3(宽限到期)或4(网关兜底)，got %d", code)
	}
	t.Logf("场景=网关多路停机一路卡住 判定依据=Shutdown按时返回;active=0;OnConnEnd code=%d", code)
}

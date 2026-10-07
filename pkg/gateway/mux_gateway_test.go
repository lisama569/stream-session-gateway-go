package gateway

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/session"
)

// TestGatewayMultiplexedStreams：经真实监听端口，在一条连接上跑多路 echo，
// 验证网关级 StreamHandler 接线、路内保序，以及 Shutdown 干净收尾。
func TestGatewayMultiplexedStreams(t *testing.T) {
	o := testOpts()
	cfg := Config{
		Listen:        "127.0.0.1:0",
		Session:       o,
		ShutdownGrace: time.Second,
		StreamHandler: session.StreamHandlerFunc(func(ctx context.Context, st *session.Stream, f *frame.Frame) {
			if err := st.Send(ctx, f); err != nil {
				t.Errorf("mux echo: %v", err)
			}
		}),
	}
	g := New(nil, cfg)
	g.SetLogger(log.Printf)
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", g.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const streams = 6
	const per = 10
	var raw bytes.Buffer
	for seq := 0; seq < per; seq++ {
		for id := uint32(1); id <= streams; id++ {
			b, _ := frame.Append(nil, &frame.Frame{Version: frame.V2, Stream: id,
				Payload: []byte(fmt.Sprintf("c%dm%d", id, seq))})
			raw.Write(b)
		}
	}
	if _, err := conn.Write(raw.Bytes()); err != nil {
		t.Fatal(err)
	}

	got := map[uint32][]string{}
	var mu sync.Mutex
	dec := frame.NewDecoder(conn)
	total := streams * per
	_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	for i := 0; i < total; i++ {
		var gf frame.Frame
		if err := dec.Next(&gf); err != nil {
			t.Fatalf("read echo %d: %v", i, err)
		}
		mu.Lock()
		got[gf.Stream] = append(got[gf.Stream], string(gf.Payload))
		mu.Unlock()
	}
	mu.Lock()
	for id := uint32(1); id <= streams; id++ {
		if len(got[id]) != per {
			mu.Unlock()
			t.Fatalf("stream %d count=%d want %d", id, len(got[id]), per)
		}
		for seq := 0; seq < per; seq++ {
			want := fmt.Sprintf("c%dm%d", id, seq)
			if got[id][seq] != want {
				mu.Unlock()
				t.Fatalf("stream %d reorder at %d: %s want %s", id, seq, got[id][seq], want)
			}
		}
	}
	mu.Unlock()

	// 所有流仍活跃（未半关闭），Shutdown 应在宽限内干净收尾（handler 均已返回）。
	if g.ActiveSessions() != 1 {
		t.Fatalf("active sessions=%d want 1", g.ActiveSessions())
	}
	if err := g.Shutdown(500 * time.Millisecond); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	log.Printf("场景=网关多路集成 路数=%d 每路=%d 判定依据=60帧按路按序回显且Shutdown干净 active=%d",
		streams, per, g.ActiveSessions())
}

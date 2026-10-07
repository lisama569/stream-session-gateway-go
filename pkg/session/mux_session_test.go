package session

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/mux"
)

// newMuxTCPPair 建立一个启用多路复用的真实 TCP 会话。
func newMuxTCPPair(t *testing.T, opts Options, h StreamHandler) pipePair {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ac := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ac <- c
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server := <-ac
	_ = ln.Close()
	s := New(2, server, nil, opts)
	s.SetStreamHandler(h)
	go func() { _ = s.Run(context.Background()) }()
	return pipePair{s: s, client: client}
}

func muxEchoOpts() Options {
	o := DefaultOptions()
	o.Mux = muxDefaultCfg()
	return o
}

func muxDefaultCfg() mux.Config { return mux.DefaultConfig() }

// TestMuxEndToEndConcurrencyEcho：多条逻辑流在一条连接上并发 echo，
// 验证端到端多路分发、路内保序与内容正确。
func TestMuxEndToEndConcurrencyEcho(t *testing.T) {
	h := StreamHandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if err := st.Send(ctx, f); err != nil {
			t.Errorf("echo send stream=%d: %v", st.ID(), err)
		}
	})
	p := newMuxTCPPair(t, muxEchoOpts(), h)
	defer p.client.Close()
	defer p.s.ForceClose(ReasonForcedByCaller)

	const streams = 5
	const per = 12
	// 客户端交错写入多路帧（带粘包：拼成一个大 buffer 一次写）。
	var raw bytes.Buffer
	for seq := 0; seq < per; seq++ {
		for id := uint32(1); id <= streams; id++ {
			b, _ := frame.Append(nil, &frame.Frame{
				Version: frame.V2, Stream: id,
				Payload: []byte(fmt.Sprintf("stream-%d-seq-%d", id, seq)),
			})
			raw.Write(b)
		}
	}
	go func() { _, _ = p.client.Write(raw.Bytes()) }()

	// 按流收集回显并核对顺序。
	got := map[uint32][]string{}
	var mu sync.Mutex
	dec := frame.NewDecoder(p.client)
	total := streams * per
	done := make(chan struct{})
	go func() {
		var gf frame.Frame
		for i := 0; i < total; i++ {
			if err := dec.Next(&gf); err != nil {
				return
			}
			cp := string(gf.Payload)
			mu.Lock()
			got[gf.Stream] = append(got[gf.Stream], cp)
			mu.Unlock()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("echo not complete: %v", len(got))
	}
	mu.Lock()
	defer mu.Unlock()
	for id := uint32(1); id <= streams; id++ {
		arr := got[id]
		if len(arr) != per {
			t.Fatalf("stream %d got %d want %d", id, len(arr), per)
		}
		for seq := 0; seq < per; seq++ {
			want := fmt.Sprintf("stream-%d-seq-%d", id, seq)
			if arr[seq] != want {
				t.Fatalf("stream %d seq %d = %q want %q (reorder)", id, seq, arr[seq], want)
			}
		}
	}
	log.Printf("场景=端到端多路echo 路数=%d 每路=%d 活跃=%d 判定依据=各路帧按序号原样返回无串包乱序",
		streams, per, p.s.ActiveStreams())
}

// TestMuxBadBytesInterleavedRecovery：坏帧（坏校验/未知版本）穿插在多路好帧
// 之间，解码器重同步后各路归属与顺序仍正确，好帧一个不丢。
func TestMuxBadBytesInterleavedRecovery(t *testing.T) {
	var got int64
	h := StreamHandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		atomic.AddInt64(&got, 1)
		_ = st.Send(ctx, f)
	})
	p := newMuxTCPPair(t, muxEchoOpts(), h)
	defer p.client.Close()
	defer p.s.ForceClose(ReasonForcedByCaller)

	enc := func(f *frame.Frame) []byte {
		b, _ := frame.Append(nil, f)
		return b
	}
	good := func(id uint32, seq int) *frame.Frame {
		return &frame.Frame{Version: frame.V2, Stream: id, Payload: []byte(fmt.Sprintf("g%d-%d", id, seq))}
	}
	// 故意构造坏帧：合法头但把校验字节改坏。
	bad := func(ver frame.Version) []byte {
		b := append([]byte(nil), enc(&frame.Frame{Version: ver, Stream: 77, Payload: []byte("X")})...)
		b[len(b)-1] ^= 0xFF
		return b
	}

	var raw bytes.Buffer
	raw.Write(enc(good(1, 0)))
	raw.Write(bad(frame.V2)) // 坏校验，夹在 s1 与 s2 之间
	raw.Write(enc(good(2, 0)))
	raw.Write([]byte{frame.SOF, 9, 0, 0, 1, 0, 0, 0, 0, 'Z'}) // 未知版本 9
	raw.Write(enc(good(1, 1)))
	raw.Write(bad(frame.V1))
	raw.Write(enc(good(2, 1)))
	go func() { _, _ = p.client.Write(raw.Bytes()) }()

	want := map[uint32][]string{
		1: {"g1-0", "g1-1"},
		2: {"g2-0", "g2-1"},
	}
	collected := map[uint32][]string{}
	var mu sync.Mutex
	dec := frame.NewDecoder(p.client)
	deadline := time.After(4 * time.Second)
	for len(collected[1])+len(collected[2]) < 4 {
		var gf frame.Frame
		_ = p.client.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err := dec.Next(&gf); err != nil {
			if err == io.EOF {
				break
			}
			continue // 坏帧在服务端被重同步；客户端只读回显的好帧
		}
		mu.Lock()
		collected[gf.Stream] = append(collected[gf.Stream], string(gf.Payload))
		mu.Unlock()
		select {
		case <-deadline:
			t.Fatalf("timeout collected=%v", collected)
		default:
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for id, ws := range want {
		if got := joinStrings(collected[id]); got != joinStrings(ws) {
			t.Fatalf("stream %d = %v want %v after resync", id, collected[id], ws)
		}
	}
	if atomic.LoadInt64(&got) != 4 {
		t.Fatalf("server processed %d good frames want 4 (no good frame lost)", atomic.LoadInt64(&got))
	}
	log.Printf("场景=坏字节穿插多路恢复 处理好帧=%d 判定依据=3类坏帧重同步后s1/s2各2帧归属与顺序正确", atomic.LoadInt64(&got))
}

func joinStrings(a []string) string {
	var b bytes.Buffer
	for i, s := range a {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s)
	}
	return b.String()
}

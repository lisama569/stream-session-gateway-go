package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
)

// pipePair 封装一条服务端会话与一个“客户端”裸连接。
type pipePair struct {
	s      *Session
	client net.Conn
}

func newPair(t *testing.T, opts Options, h Handler) pipePair {
	t.Helper()
	server, client := net.Pipe()
	s := New(1, server, h, opts)
	go func() { _ = s.Run(context.Background()) }()
	return pipePair{s: s, client: client}
}

// newTCPPair 用真实本地 TCP 建立会话，支持半关闭（CloseWrite/FIN）语义。
func newTCPPair(t *testing.T, opts Options, h Handler) pipePair {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	type accept struct {
		c   net.Conn
		err error
	}
	ac := make(chan accept, 1)
	go func() {
		c, err := ln.Accept()
		ac <- accept{c, err}
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	a := <-ac
	if a.err != nil {
		t.Fatal(a.err)
	}
	_ = ln.Close()
	sess := New(1, a.c, h, opts)
	go func() { _ = sess.Run(context.Background()) }()
	return pipePair{s: sess, client: client}
}

func writeClientFrame(t *testing.T, c net.Conn, f *frame.Frame) {
	t.Helper()
	if err := writeClientFrameRaw(c, f); err != nil {
		t.Fatalf("client write: %v", err)
	}
}

func writeClientFrameRaw(c net.Conn, f *frame.Frame) error {
	_, err := frame.Encode(c, f)
	return err
}

func closeClientWrite(t *testing.T, c net.Conn) {
	t.Helper()
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := c.(closeWriter); ok {
		if err := cw.CloseWrite(); err != nil {
			t.Fatalf("client closewrite: %v", err)
		}
		return
	}
	c.Close()
}

// echoHandler 把每个收到的帧原样回写。
type echoHandler struct{}

func (echoHandler) OnFrame(ctx context.Context, s *Session, f *frame.Frame) {
	_ = s.Send(ctx, f)
}

func TestEchoBidirectional(t *testing.T) {
	p := newPair(t, DefaultOptions(), echoHandler{})
	defer p.client.Close()
	defer p.s.ForceClose(ReasonForcedByCaller)

	frames := []*frame.Frame{
		{Version: frame.V1, Stream: 1, Payload: []byte("ping-1")},
		{Version: frame.V2, Stream: 2, Payload: bytes.Repeat([]byte("z"), 4096)},
		{Version: frame.V2, Stream: 3, Payload: nil},
	}
	var raw bytes.Buffer
	for _, f := range frames {
		b, _ := frame.Append(nil, f)
		raw.Write(b)
	}
	go func() { _, _ = p.client.Write(raw.Bytes()) }()

	dec := frame.NewDecoder(p.client)
	var got frame.Frame
	for i, want := range frames {
		if err := dec.Next(&got); err != nil {
			t.Fatalf("echo frame %d: %v", i, err)
		}
		if got.Stream != want.Stream || !bytes.Equal(got.Payload, want.Payload) || got.Version != want.Version {
			t.Fatalf("frame %d mismatch: got stream=%d len=%d", i, got.Stream, len(got.Payload))
		}
	}
	log.Printf("场景=双向回显 连接状态=%s 判定依据=3帧(V1+大负载V2+空帧)全部原样返回", p.s.State())
}

func TestHalfCloseBothSides(t *testing.T) {
	p := newPair(t, DefaultOptions(), echoHandler{})
	defer p.client.Close()

	writeClientFrame(t, p.client, &frame.Frame{Version: frame.V2, Flags: frame.FlagEndStream, Payload: []byte("bye")})
	// 读到 echo 帧后应收到 END_STREAM（服务端 CloseWrite 由本用例驱动双向关闭）。
	// 这里改为直接驱动：等 handler 处理完后调用 CloseWrite。
	time.Sleep(100 * time.Millisecond)
	if err := p.s.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	dec := frame.NewDecoder(p.client)
	var got frame.Frame
	sawEnd := false
	for i := 0; i < 3; i++ {
		err := dec.Next(&got)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if got.EndStream() {
			sawEnd = true
		}
	}
	if err := p.s.Wait(); err != nil {
		t.Fatalf("session should end normally, got %v", err)
	}
	if p.s.State() != StateClosed {
		t.Fatalf("want closed, got %s", p.s.State())
	}
	if !sawEnd {
		t.Fatal("client must observe END_STREAM frame")
	}
	log.Printf("场景=双向半关闭 连接状态=%s reason=%s 判定依据=END_STREAM到达且Wait无错误", p.s.State(), p.s.Reason())
}

func TestIdleTimeout(t *testing.T) {
	opts := DefaultOptions()
	opts.IdleTimeout = 120 * time.Millisecond
	p := newPair(t, opts, echoHandler{})
	defer p.client.Close()

	err := p.s.Wait()
	if err == nil {
		t.Fatal("idle timeout should force-terminate with error")
	}
	if p.s.State() != StateForceTerminated {
		t.Fatalf("want force-terminated, got %s", p.s.State())
	}
	if p.s.Reason().Code != ReasonIdleTimeout.Code {
		t.Fatalf("want idle reason, got %s", p.s.Reason())
	}
	log.Printf("场景1=空闲超时 连接状态=%s 判定依据=%s", p.s.State(), p.s.Reason())

	// 有帧活动时不应误杀。
	opts2 := DefaultOptions()
	opts2.IdleTimeout = 100 * time.Millisecond
	p2 := newPair(t, opts2, echoHandler{})
	defer p2.client.Close()
	defer p2.s.ForceClose(ReasonForcedByCaller)
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(30 * time.Millisecond)
		defer tk.Stop()
		i := uint32(0)
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				writeClientFrame(t, p2.client, &frame.Frame{Version: frame.V2, Stream: atomic.AddUint32(&i, 1), Payload: []byte("a")})
			}
		}
	}()
	time.Sleep(350 * time.Millisecond)
	close(stop)
	if p2.s.State() != StateActive {
		t.Fatalf("active session with traffic must stay active, got %s", p2.s.State())
	}
	log.Printf("场景2=持续有流量 连接状态=%s 判定依据=3倍超时后仍active", p2.s.State())
}

func TestForceCloseIdempotent(t *testing.T) {
	p := newPair(t, DefaultOptions(), echoHandler{})
	defer p.client.Close()
	r1 := CloseReason{Code: 77, Text: "first", Forced: true}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = p.s.ForceClose(r1) }()
	}
	wg.Wait()
	if err := p.s.Wait(); err == nil {
		t.Fatal("force close must produce non-nil final error")
	}
	if got := p.s.Reason(); got.Code != 77 {
		t.Fatalf("first reason must be preserved, got %s", got)
	}
	if p.s.State() != StateForceTerminated {
		t.Fatalf("state=%s", p.s.State())
	}
	// 终止后 Send/CloseWrite/GracefulClose 必须安全。
	if err := p.s.Send(context.Background(), &frame.Frame{Version: frame.V2}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after close: %v", err)
	}
	if err := p.s.CloseWrite(); !errors.Is(err, ErrClosed) {
		t.Fatalf("CloseWrite after close: %v", err)
	}
	if err := p.s.GracefulClose(time.Millisecond); err != nil {
		t.Fatalf("GracefulClose after terminate must be no-op, got %v", err)
	}
	log.Printf("场景=重复强制关闭竞态 连接状态=%s reason=%s 判定依据=16路并发ForceClose保留首次原因且无panic", p.s.State(), p.s.Reason())
}

func TestGracefulDrain(t *testing.T) {
	var processed int64
	h := HandlerFunc(func(ctx context.Context, s *Session, f *frame.Frame) {
		time.Sleep(60 * time.Millisecond) // 模拟在途处理
		atomic.AddInt64(&processed, 1)
		_ = s.Send(ctx, f)
	})
	p := newTCPPair(t, DefaultOptions(), h)
	defer p.client.Close()

	// 客户端并发读回显帧，避免 net.Pipe 写阻塞。
	clientDec := frame.NewDecoder(p.client)
	clientGot := make(chan int, 8)
	go func() {
		var gf frame.Frame
		for {
			if err := clientDec.Next(&gf); err != nil {
				return
			}
			clientGot <- 1
		}
	}()
	go func() {
		if err := writeClientFrameRaw(p.client, &frame.Frame{Version: frame.V1, Stream: 1, Payload: []byte("a")}); err != nil {
			t.Errorf("client write a: %v", err)
		}
	}()
	go func() {
		if err := writeClientFrameRaw(p.client, &frame.Frame{Version: frame.V1, Stream: 2, Payload: []byte("b")}); err != nil {
			t.Errorf("client write b: %v", err)
		}
	}()
	time.Sleep(30 * time.Millisecond) // 确保帧已派发
	if err := p.s.GracefulClose(500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// 正确的优雅顺序：客户端发完数据后关闭写方向（FIN），
	// 服务端读循环立即得到 EOF，无需耗满宽限。
	closeClientWrite(t, p.client)
	deadline := time.After(2 * time.Second)
	for echo := 0; echo < 2; {
		select {
		case <-clientGot:
			echo++
		case <-deadline:
			t.Fatalf("echo frames lost, got %d/2", echo)
		}
	}
	if err := p.s.Wait(); err != nil {
		t.Fatalf("drain within grace must close normally: %v", err)
	}
	if p.s.State() != StateClosed {
		t.Fatalf("want closed, got %s", p.s.State())
	}
	if atomic.LoadInt64(&processed) != 2 {
		t.Fatalf("inflight work must not be lost: processed=%d", processed)
	}
	// 排空期间 Send 被拒。
	if err := p.s.Send(context.Background(), &frame.Frame{Version: frame.V2}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send during/after drain: %v", err)
	}
	log.Printf("场景=宽限排空 连接状态=%s processed=%d 判定依据=在途2帧处理完才关闭且不丢失", p.s.State(), processed)
}

func TestGraceExpiryForceTerminates(t *testing.T) {
	release := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, s *Session, f *frame.Frame) {
		// 模拟排空期间无法及时完成的处理器；但仍尊重 ctx，
		// 强制终止后释放（不泄漏 goroutine）。
		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	p := newPair(t, DefaultOptions(), h)
	defer p.client.Close()
	defer close(release)

	writeClientFrame(t, p.client, &frame.Frame{Version: frame.V1, Payload: []byte("stuck")})
	time.Sleep(50 * time.Millisecond)
	_ = p.s.GracefulClose(150 * time.Millisecond)
	err := p.s.Wait()
	if err == nil || p.s.State() != StateForceTerminated {
		t.Fatalf("grace expiry must force-terminate, state=%s err=%v", p.s.State(), err)
	}
	if p.s.Reason().Code != ReasonGraceExpired.Code {
		t.Fatalf("want grace-expired reason, got %s", p.s.Reason())
	}
	log.Printf("场景=宽限超时 连接状态=%s 判定依据=%s", p.s.State(), p.s.Reason())
}

func TestInboundBackpressureBounded(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxInflightFrames = 4
	opts.MaxInflightBytes = 1 << 30
	opts.InflightPolicy = PolicyBlock
	opts.ConcurrentHandlers = true

	var inflight int64
	var peak int64
	var processed int64
	proceed := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, s *Session, f *frame.Frame) {
		cur := atomic.AddInt64(&inflight, 1)
		for {
			pk := atomic.LoadInt64(&peak)
			if cur <= pk || atomic.CompareAndSwapInt64(&peak, pk, cur) {
				break
			}
		}
		<-proceed
		atomic.AddInt64(&inflight, -1)
		atomic.AddInt64(&processed, 1)
	})
	p := newPair(t, opts, h)
	defer p.client.Close()
	defer p.s.ForceClose(ReasonForcedByCaller)

	// 塞入 20 帧（net.Pipe 同步，必须用独立 writer，背压时它会阻塞在写）。
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < 20; i++ {
			if err := writeClientFrameRaw(p.client, &frame.Frame{Version: frame.V1, Stream: uint32(i), Payload: []byte("x")}); err != nil {
				t.Errorf("client write %d: %v", i, err)
				return
			}
		}
	}()
	// 等待在途数填满到配额 4。
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt64(&peak) < 4 {
		select {
		case <-deadline:
			t.Fatalf("inflight should fill to quota 4, peak=%d", atomic.LoadInt64(&peak))
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	time.Sleep(150 * time.Millisecond) // 证明读已背压暂停、不会突破上限
	if pk := atomic.LoadInt64(&peak); pk > 4 {
		t.Fatalf("inflight exceeded quota: peak=%d", pk)
	}
	close(proceed)
	select {
	case <-writerDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("writer stuck: backpressure did not release, processed=%d", atomic.LoadInt64(&processed))
	}
	// writer 完成仅代表 20 帧都已写入管道；解码器残余字节仍需读完。
	deadline2 := time.After(3 * time.Second)
	for atomic.LoadInt64(&processed) != 20 {
		select {
		case <-deadline2:
			t.Fatalf("frames stuck in decoder buffer, processed=%d", atomic.LoadInt64(&processed))
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	log.Printf("场景=入站背压 峰值在途=%d 最终处理=%d 判定依据=超过配额后读取暂停, 释放后20帧全部处理",
		atomic.LoadInt64(&peak), atomic.LoadInt64(&processed))
}

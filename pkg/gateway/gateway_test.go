package gateway

import (
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
	"github.com/lisama569/stream-session-gateway-go/pkg/session"
)

func testOpts() session.Options {
	o := session.DefaultOptions()
	o.WriteTimeout = 2 * time.Second
	return o
}

func testGateway(t *testing.T, cfg Config) *Gateway {
	t.Helper()
	g := New(session.HandlerFunc(func(ctx context.Context, s *session.Session, f *frame.Frame) {
		_ = s.Send(ctx, f) // echo
	}), cfg)
	g.SetLogger(log.Printf)
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return g
}

func dialAndCheck(t *testing.T, addr net.Addr, n int) {
	t.Helper()
	conns := make([]net.Conn, n)
	defer func() {
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
	}()
	var ok int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wgEnd(&wg)
			c, err := net.Dial("tcp", addr.String())
			if err != nil {
				return // 被拒是预期的
			}
			conns[i] = c
			// 通过完成一次 echo 验证连接真正进入运行态。
			f := &frame.Frame{Version: frame.V2, Stream: uint32(i), Payload: []byte("x")}
			if _, err := frame.Encode(c, f); err != nil {
				c.Close()
				return
			}
			dec := frame.NewDecoder(c)
			var got frame.Frame
			if err := dec.Next(&got); err != nil {
				c.Close()
				return
			}
			atomic.AddInt64(&ok, 1)
		}(i)
	}
	wg.Wait()
	if atomic.LoadInt64(&ok) != int64(n) {
		t.Fatalf("all %d concurrent clients must get echo, got %d", n, atomic.LoadInt64(&ok))
	}
}

func wgEnd(wg *sync.WaitGroup) { wg.Done() }

func TestConcurrentAccept(t *testing.T) {
	cfg := Config{Listen: "127.0.0.1:0", Session: testOpts()}
	g := testGateway(t, cfg)
	defer g.Shutdown(time.Second)

	const n = 40
	dialAndCheck(t, g.ListenAddr(), n)
	if got := g.Stats().Accepted; got != n {
		t.Fatalf("accepted=%d want %d", got, n)
	}
	log.Printf("场景=并发接入 连接状态=%d个会话全部echo成功 判定依据=Accepted=%d", n, g.Stats().Accepted)
}

func TestConnectionQuotaReject(t *testing.T) {
	cfg := Config{
		Listen:         "127.0.0.1:0",
		MaxConnections: 3,
		Session: func() session.Options {
			o := testOpts()
			o.IdleTimeout = 0
			return o
		}(),
	}
	g := testGateway(t, cfg)
	defer g.Shutdown(time.Second)

	// 先占满 3 个长连接（发一帧建立会话但不关闭，保持在途）。
	holders := make([]net.Conn, 3)
	for i := range holders {
		c, err := net.Dial("tcp", g.ListenAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		holders[i] = c
		if _, err := frame.Encode(c, &frame.Frame{Version: frame.V2, Payload: []byte("hold")}); err != nil {
			t.Fatal(err)
		}
	}
	// 等会话全部建立。
	deadline := time.After(2 * time.Second)
	for g.ActiveSessions() != 3 {
		select {
		case <-deadline:
			t.Fatalf("active=%d want 3", g.ActiveSessions())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	// 溢出的连接要么拨号失败，要么被立即关闭，且绝不能 echo。
	var rejected int64
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := net.Dial("tcp", g.ListenAddr().String())
			if err != nil {
				atomic.AddInt64(&rejected, 1)
				return
			}
			_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			buf := make([]byte, 16)
			_, err = c.Read(buf)
			if err != nil {
				atomic.AddInt64(&rejected, 1)
			}
			c.Close()
		}()
	}
	wg.Wait()
	if atomic.LoadInt64(&rejected) != 10 {
		t.Fatalf("overflow connections must be rejected, got %d/10", rejected)
	}
	if g.Stats().RejectedConnQuota != 10 {
		t.Fatalf("RejectedConnQuota=%d want 10", g.Stats().RejectedConnQuota)
	}
	for _, c := range holders {
		c.Close()
	}
	log.Printf("场景=连接配额超限 连接状态=3常驻+10溢出 判定依据=溢出10个全部无echo且RejectedConnQuota=%d",
		g.Stats().RejectedConnQuota)
}

func TestGlobalBytesQuotaReject(t *testing.T) {
	var endedMu sync.Mutex
	var code13 int64
	o := testOpts()
	o.MaxInflightBytes = 0 // 会话级不单独限制，只看全局
	o.MaxOutboundBytes = 0
	o.InflightPolicy = session.PolicyReject
	cfg := Config{
		Listen:      "127.0.0.1:0",
		GlobalBytes: 256, // 全局在途字节预算极小
		Session:     o,
		OnSessionEnd: func(id uint64, st session.State, r session.CloseReason) {
			endedMu.Lock()
			if r.Code == 13 {
				atomic.AddInt64(&code13, 1)
			}
			endedMu.Unlock()
		},
	}
	g := testGateway(t, cfg)
	defer g.Shutdown(time.Second)

	var gotReason atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", g.ListenAddr().String())
			if err != nil {
				return
			}
			defer c.Close()
			// 发送 1KB 负载的大帧，超过全局预算，会话应因配额原因(code=13)终止。
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, _ = frame.Encode(c, &frame.Frame{Version: frame.V2, Stream: uint32(i), Payload: make([]byte, 1024)})
			dec := frame.NewDecoder(c)
			var f frame.Frame
			if err := dec.Next(&f); err != nil {
				if errors.Is(err, io.EOF) {
					gotReason.Store(true) // 会话被终止→连接关闭
				}
			}
		}(i)
	}
	wg.Wait()
	if atomic.LoadInt64(&code13) == 0 {
		t.Fatalf("at least one session must end with quota reason code=13, got %d", code13)
	}
	log.Printf("场景=全局字节配额 连接状态=大帧会话以code13终止 判定依据=OnSessionEnd计数=%d", atomic.LoadInt64(&code13))
}

func TestShutdownRace(t *testing.T) {
	o := testOpts()
	o.GracePeriod = 400 * time.Millisecond
	cfg := Config{Listen: "127.0.0.1:0", Session: o}
	g := New(session.HandlerFunc(func(ctx context.Context, s *session.Session, f *frame.Frame) {
		_ = s.Send(ctx, f)
	}), cfg)
	g.SetLogger(log.Printf)
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 建立一个带在途活动的长会话。
	c, err := net.Dial("tcp", g.ListenAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	keepGoing := make(chan struct{})
	var echoOK atomic.Int64
	go func() {
		dec := frame.NewDecoder(c)
		var f frame.Frame
		for {
			if err := dec.Next(&f); err != nil {
				return
			}
			echoOK.Add(1)
			select {
			case <-keepGoing:
				return
			default:
			}
		}
	}()
	go func() {
		i := 0
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-keepGoing:
				return
			case <-tk.C:
				_, _ = frame.Encode(c, &frame.Frame{Version: frame.V2, Stream: uint32(i), Payload: []byte("p")})
				i++
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)

	// 停机：重复调用必须幂等；并发 Shutdown 不崩溃。
	var swg sync.WaitGroup
	for i := 0; i < 8; i++ {
		concurrentShutdown(&swg, g)
	}
	swg.Wait()
	close(keepGoing)
	_ = c.Close()

	// 停机后新连接必须被拒绝。
	if _, err := net.DialTimeout("tcp", g.ListenAddr().String(), 500*time.Millisecond); err == nil {
		t.Fatal("new connection after shutdown must fail")
	}
	if got := g.ActiveSessions(); got != 0 {
		t.Fatalf("all sessions must end after shutdown, active=%d", got)
	}
	log.Printf("场景=停机竞态 连接状态=active=%d 停机前echo=%d 判定依据=8路并发Shutdown无panic且新连接被拒",
		g.ActiveSessions(), echoOK.Load())
}

func concurrentShutdown(wg *sync.WaitGroup, g *Gateway) {
	wg.Add(1)
	go func() { defer wg.Done(); _ = g.Shutdown(400 * time.Millisecond) }()
}

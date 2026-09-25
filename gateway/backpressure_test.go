package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// gatedHandler：前 gateN 帧阻塞直到 open 关闭；统计处理次数以验证不丢不重。
type gatedHandler struct {
	open    chan struct{}
	gateN   int
	mu      sync.Mutex
	started map[uint32]int
	done    map[uint32]int
}

func newGatedHandler(gateN int) *gatedHandler {
	return &gatedHandler{
		open:    make(chan struct{}),
		gateN:   gateN,
		started: map[uint32]int{},
		done:    map[uint32]int{},
	}
}

func (h *gatedHandler) ServeFrame(sess *Session, f *Frame) error {
	h.mu.Lock()
	h.started[f.Seq]++
	seq := f.Seq
	h.mu.Unlock()
	if int(seq) <= h.gateN {
		<-h.open // 前几帧持有配额不释放
	}
	h.mu.Lock()
	h.done[seq]++
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return sess.Send(ctx, f)
}

func TestE2EBackpressureNoLossNoDup(t *testing.T) {
	t.Logf("场景: PolicyBlock，单会话在途帧数=2；前 2 帧被 Handler 阻塞，" +
		"第 3 帧应在配额释放后处理，不被拒绝、不丢失、不重复")
	h := newGatedHandler(2)
	cfg := Config{
		GracePeriod: 2 * time.Second,
		Handler:     h,
		Quotas: QuotaConfig{
			GlobalConcurrent:      8,
			SessionInflightFrames: 2,
			Policy:                PolicyBlock,
		},
	}
	g, codec := startTestGateway(t, cfg)
	defer g.ForceShutdown("test end")

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := newFrameReader(t, codec, conn)

	send := func(seq uint32) {
		w, _ := codec.Encode(&Frame{Version: V2, Type: TypeData,
			Seq: seq, Payload: []byte{byte(seq)}})
		if _, err := conn.Write(w); err != nil {
			t.Fatal(err)
		}
	}
	send(1)
	send(2)
	send(3)

	// 等待前 2 帧进入处理；第 3 帧因背压必须等待（不拒绝）。
	deadline := time.Now().Add(time.Second)
	for {
		h.mu.Lock()
		n := len(h.started)
		h.mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("前 2 帧未进入处理")
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond) // 确认第 3 帧没有被提前处理或拒绝
	h.mu.Lock()
	if h.started[3] != 0 {
		h.mu.Unlock()
		t.Fatal("背压失效：第 3 帧在配额满时被处理")
	}
	h.mu.Unlock()
	// 非破坏式探测：不调用 Read，避免把前两帧的回显字节从连接里读走。
	// 用 started 计数确认第 3 帧尚未被处理即可。
	time.Sleep(100 * time.Millisecond)
	_ = conn.SetReadDeadline(time.Time{}) // 不设置任何截止时间，正式读取用独立 deadline
	t.Logf("连接状态: 前 2 帧处理中持有 2/2 配额，第 3 帧背压等待（未拒绝、未处理）")

	close(h.open) // 释放配额

	// 三帧都应被回显且各一次。
	seen := map[uint32]int{}
	rejects := 0
	for i := 0; i < 3; i++ {
		got, derr := reader.next(time.Now().Add(2 * time.Second))
		if derr != nil {
			t.Fatalf("第 %d 帧丢失: %v", i, derr)
		}
		if got.Type == TypeReject {
			rejects++
			continue
		}
		seen[got.Seq]++
	}
	if rejects != 0 {
		t.Fatalf("Block 策略下不应有拒绝帧, rejects=%d", rejects)
	}
	for seq := uint32(1); seq <= 3; seq++ {
		if seen[seq] != 1 {
			t.Fatalf("帧 seq=%d 处理次数=%d（期望恰好 1，不丢不重）", seq, seen[seq])
		}
	}
	t.Logf("判定: 背压解除后帧 1/2/3 各处理恰好一次（不丢失、不重复、未拒绝）")
}

func TestE2EGlobalBytesBackpressure(t *testing.T) {
	t.Logf("场景: PolicyBlock，全局在途字节=16；两个会话共持有配额后，新帧等待全局字节释放")
	h := newGatedGlobal()
	cfg := Config{
		GracePeriod: 2 * time.Second,
		Handler:     h,
		Quotas: QuotaConfig{
			GlobalConcurrent:    8,
			GlobalInflightBytes: 16,
			Policy:              PolicyBlock,
		},
	}
	g, codec := startTestGateway(t, cfg)
	defer g.ForceShutdown("test end")

	dial := func() net.Conn {
		c, err := net.Dial("tcp", g.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c1, c2 := dial(), dial()
	defer c1.Close()
	defer c2.Close()

	writeFrame := func(c net.Conn, seq uint32, n int) {
		w, _ := codec.Encode(&Frame{Version: V2, Type: TypeData,
			Seq: seq, Payload: make([]byte, n)})
		if _, err := c.Write(w); err != nil {
			t.Fatal(err)
		}
	}
	writeFrame(c1, 1, 16) // 占满全局 16B
	<-h.firstCh
	writeFrame(c2, 2, 8) // 必须背压等待
	time.Sleep(80 * time.Millisecond)
	h.mu.Lock()
	started := h.started[2]
	h.mu.Unlock()
	if started {
		t.Fatal("全局字节配额满时第 2 个会话的帧不应开始处理")
	}
	t.Logf("连接状态: 全局字节 16/16 被会话1持有，会话2帧背压等待")

	close(h.open)
	got, derr := readOneFrame(t, codec, c2, time.Now().Add(2*time.Second))
	if derr != nil || got.Seq != 2 {
		t.Fatalf("释放后期望 seq=2 回显, got=%+v err=%v", got, derr)
	}
	t.Logf("判定: 全局字节释放后背压解除，会话2 seq=2 正常回显")
}

// gatedGlobal 让首个帧阻塞，并通过 started 标记后续帧是否启动。
type gatedGlobal struct {
	open    chan struct{}
	firstCh chan struct{}
	once    sync.Once
	mu      sync.Mutex
	started map[uint32]bool
}

func newGatedGlobal() *gatedGlobal {
	return &gatedGlobal{open: make(chan struct{}), firstCh: make(chan struct{})}
}

func (g *gatedGlobal) ServeFrame(sess *Session, f *Frame) error {
	g.once.Do(func() { close(g.firstCh); <-g.open })
	g.mu.Lock()
	if g.started == nil {
		g.started = map[uint32]bool{}
	}
	g.started[f.Seq] = true
	g.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return sess.Send(ctx, f)
}

var _ = context.Background

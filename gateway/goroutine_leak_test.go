package gateway

import (
	"net"
	"runtime"
	"testing"
	"time"
)

// stallHandler 永远阻塞，用来让会话的后续帧卡在 PolicyBlock 配额申请上。
type stallHandler struct{ hold chan struct{} }

func (h stallHandler) ServeFrame(sess *Session, f *Frame) error {
	<-h.hold
	return nil
}

func TestE2EForceShutdownUnblocksBackpressure(t *testing.T) {
	t.Logf("场景: PolicyBlock 且会话帧数=1，首帧 Handler 永久阻塞持有配额；" +
		"第 2 帧的读 goroutine 阻塞在配额申请上。ForceShutdown 必须解除阻塞并回收全部 goroutine")

	hold := make(chan struct{})
	g, codec := startTestGateway(t, Config{
		GracePeriod: time.Second,
		Handler:     stallHandler{hold: hold},
		Quotas: QuotaConfig{
			GlobalConcurrent:      4,
			SessionInflightFrames: 1,
			Policy:                PolicyBlock,
		},
	})

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	w1, _ := codec.Encode(&Frame{Version: V2, Type: TypeData, Seq: 1, Payload: []byte("a")})
	w2, _ := codec.Encode(&Frame{Version: V2, Type: TypeData, Seq: 2, Payload: []byte("bb")})
	if _, err := conn.Write(w1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 首帧进入 Handler 永久阻塞
	if _, err := conn.Write(w2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 第 2 帧读 goroutine 阻塞在 Acquire
	t.Logf("连接状态: seq=1 处理中(持有 1/1)，seq=2 读 goroutine 背压阻塞")

	before := runtime.NumGoroutine()
	if err := g.ForceShutdown("leak test"); err != nil {
		t.Logf("ForceShutdown: %v", err)
	}

	if !g.waitSessions(2 * time.Second) {
		t.Fatal("判定失败: 强制停机后仍有会话 goroutine 未退出（泄漏/挂起）")
	}
	// 让被唤醒后可能新建的短暂 goroutine 退出。
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	t.Logf("判定: 会话 goroutine 全部退出；goroutine 数 before=%d after=%d", before, after)
	if after > before+2 {
		t.Fatalf("疑似 goroutine 泄漏: before=%d after=%d", before, after)
	}
	close(hold) // 释放永久阻塞的 Handler
}

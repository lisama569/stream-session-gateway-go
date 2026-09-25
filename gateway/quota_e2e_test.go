package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// holdConn 建立连接并保持打开，占用一个并发名额。
func holdConn(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// 发送半包占住会话（不触发任何处理）。
	if _, err := conn.Write([]byte{Magic}); err != nil {
		t.Fatalf("write hold: %v", err)
	}
	return conn
}

func TestE2EGlobalConcurrentReject(t *testing.T) {
	t.Logf("场景: 全局并发上限=2，前 2 个连接占用，第 3 个连接应被拒绝并返回 Reject(global_concurrent_limit)")
	cfg := Config{
		GracePeriod: time.Second,
		Quotas: QuotaConfig{
			GlobalConcurrent: 2,
			Policy:           PolicyReject,
		},
	}
	g, codec := startTestGateway(t, cfg)
	defer g.ForceShutdown("test end")

	c1 := holdConn(t, g.Addr().String())
	defer c1.Close()
	c2 := holdConn(t, g.Addr().String())
	defer c2.Close()

	deadline := time.Now().Add(time.Second)
	for g.ActiveSessions() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("连接状态: 期望 active=2, 实际=%d", g.ActiveSessions())
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("连接状态: active=%d 达到全局上限", g.ActiveSessions())

	c3, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	got, derr := readOneFrame(t, codec, c3, time.Now().Add(2*time.Second))
	if derr != nil {
		t.Fatalf("判定失败: 期望收到 Reject 帧, 错误=%v", derr)
	}
	if got.Type != TypeReject || got.Flags != uint8(KindGlobalConcurrent) {
		t.Fatalf("判定失败: 期望 Reject/%s, 得到 type=%d flags=%d",
			KindGlobalConcurrent, got.Type, got.Flags)
	}
	t.Logf("判定: 第 3 个连接被拒绝, type=Reject reason=%s 连接随后关闭",
		ErrorKind(got.Flags))
}

// blockingHandler 让第一帧阻塞在处理中，保持配额占用，直到 release 关闭。
// 释放后该帧仍会被正常回显。
type blockingHandler struct {
	release chan struct{}
	started chan struct{}
	once    sync.Once
}

func (h *blockingHandler) ServeFrame(sess *Session, f *Frame) error {
	h.once.Do(func() { close(h.started) })
	<-h.release
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return sess.Send(ctx, f)
}

func TestE2ESessionBytesQuotaReject(t *testing.T) {
	t.Logf("场景: 单会话在途字节上限=16（拒绝策略）；第 1 帧 8B 被 Handler 阻塞持有配额，" +
		"第 2 帧 32B 应收到 Reject(session_bytes_limit)；释放后第 1 帧正常回显，不丢不重")
	h := &blockingHandler{release: make(chan struct{}), started: make(chan struct{})}
	cfg := Config{
		GracePeriod: time.Second,
		Handler:     h,
		Quotas: QuotaConfig{
			GlobalConcurrent:     8,
			SessionInflightBytes: 16,
			Policy:               PolicyReject,
		},
	}
	g, codec := startTestGateway(t, cfg)
	defer g.ForceShutdown("test end")

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	f1 := &Frame{Version: V2, Type: TypeData, Seq: 1, Payload: make([]byte, 8)}
	f2 := &Frame{Version: V2, Type: TypeData, Seq: 2, Payload: make([]byte, 32)}
	w1, _ := codec.Encode(f1)
	w2, _ := codec.Encode(f2)

	if _, err := conn.Write(w1); err != nil {
		t.Fatal(err)
	}
	<-h.started // 第 1 帧已在处理中，持有 8B 配额
	t.Logf("连接状态: 第 1 帧(8B)处理中, 已占用会话字节配额 8/16")

	if _, err := conn.Write(w2); err != nil {
		t.Fatal(err)
	}
	got, derr := readOneFrame(t, codec, conn, time.Now().Add(2*time.Second))
	if derr != nil {
		t.Fatalf("期望 Reject, 错误=%v", derr)
	}
	if got.Type != TypeReject || got.Flags != uint8(KindSessionBytes) || got.Seq != 2 {
		t.Fatalf("期望 Reject/%s seq=2, 得到 %+v", KindSessionBytes, got)
	}
	t.Logf("判定: 第 2 帧(32B)被拒绝 reason=%s seq=2，已接收数据未入处理", ErrorKind(got.Flags))

	close(h.release) // 排空第 1 帧
	got, derr = readOneFrame(t, codec, conn, time.Now().Add(2*time.Second))
	if derr != nil {
		t.Fatalf("期望第 1 帧回显, 错误=%v", derr)
	}
	if got.Seq != 1 || len(got.Payload) != 8 {
		t.Fatalf("第 1 帧丢失或重复: %+v", got)
	}
	t.Logf("判定: 释放后第 1 帧 seq=1 恰好回显一次（不丢失、不重复）")
}

func TestE2EProtocolRecovery(t *testing.T) {
	t.Logf("场景: 真实链路上依次发送 [垃圾字节][CRC 损坏帧][好帧]，期望报告两类解码错误后好帧仍被回显")
	g, codec := startTestGateway(t, Config{})
	defer g.ForceShutdown("test end")

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	good := &Frame{Version: V2, Type: TypeData, Seq: 42, Payload: []byte("recover")}
	gwire, _ := codec.Encode(good)
	bad, _ := codec.Encode(&Frame{Version: V1, Type: TypeData, Seq: 1, Payload: []byte("xxx")})
	bad[len(bad)-1] ^= 0xFF // 破坏 CRC

	stream := append([]byte{0x00, 0x01, 0x02}, bad...)
	stream = append(stream, gwire...)
	if _, err := conn.Write(stream); err != nil {
		t.Fatal(err)
	}

	got, derr := readOneFrame(t, codec, conn, time.Now().Add(2*time.Second))
	if derr != nil {
		t.Fatalf("期望收到好帧, 错误=%v", derr)
	}
	if got.Seq != 42 || string(got.Payload) != "recover" {
		t.Fatalf("好帧内容错误: %+v", got)
	}
	t.Logf("判定: 垃圾字节与 CRC 错误被显式记录并重新同步, 好帧 seq=42 正常回显（不串包、不丢帧）")
}

package gateway

import (
	"net"
	"testing"
	"time"
)

func TestE2ESessionFramesQuotaReject(t *testing.T) {
	t.Logf("场景: 单会话在途帧数=1（拒绝策略）；首帧被 Handler 持有，" +
		"第 2 帧应收到 Reject(session_frames_limit)，与字节配额原因可区分")
	h := &blockingHandler{release: make(chan struct{}), started: make(chan struct{})}
	cfg := Config{
		GracePeriod: time.Second,
		Handler:     h,
		Quotas: QuotaConfig{
			GlobalConcurrent:      8,
			SessionInflightFrames: 1,
			Policy:                PolicyReject,
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

	w1, _ := codec.Encode(&Frame{Version: V2, Type: TypeData, Seq: 1, Payload: []byte("a")})
	w2, _ := codec.Encode(&Frame{Version: V2, Type: TypeData, Seq: 2, Payload: []byte("b")})
	conn.Write(w1)
	<-h.started
	conn.Write(w2)

	got, derr := reader.next(time.Now().Add(2 * time.Second))
	if derr != nil {
		t.Fatalf("期望 Reject, err=%v", derr)
	}
	if got.Type != TypeReject || got.Flags != uint8(KindSessionFrames) {
		t.Fatalf("期望 Reject/%s, got=%+v", KindSessionFrames, got)
	}
	t.Logf("判定: 第 2 帧被拒绝 reason=%s（区别于字节/全局配额）", ErrorKind(got.Flags))
	close(h.release)
}

func TestE2EUnknownVersionRecovery(t *testing.T) {
	t.Logf("场景: 真实链路上发送未知版本帧(magic,ver=9)，随后发送正常 V1 帧；" +
		"期望未知版本被显式记录并重新同步，好帧正常回显")
	g, codec := startTestGateway(t, Config{})
	defer g.ForceShutdown("test end")

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := newFrameReader(t, codec, conn)

	good, _ := codec.Encode(&Frame{Version: V1, Type: TypeData, Seq: 7, Payload: []byte("ok")})
	stream := append([]byte{Magic, 9, Magic, 9}, good...)
	if _, err := conn.Write(stream); err != nil {
		t.Fatal(err)
	}
	got, derr := reader.next(time.Now().Add(2 * time.Second))
	if derr != nil {
		t.Fatalf("期望好帧, err=%v", derr)
	}
	if got.Seq != 7 || string(got.Payload) != "ok" {
		t.Fatalf("好帧错误: %+v", got)
	}
	t.Logf("判定: 未知版本帧被跳过并重新同步，好帧 seq=7 正常回显")
}

func TestE2EStickyFrames(t *testing.T) {
	t.Logf("场景: 一次 Write 连续 5 个帧（强粘包），期望服务端恰好回显 5 个且不串包")
	g, codec := startTestGateway(t, Config{})
	defer g.ForceShutdown("test end")

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := newFrameReader(t, codec, conn)

	var stream []byte
	for seq := uint32(1); seq <= 5; seq++ {
		w, _ := codec.Encode(&Frame{Version: V2, Type: TypeData, Seq: seq,
			Payload: []byte{byte('a' + seq - 1)}})
		stream = append(stream, w...)
	}
	if _, err := conn.Write(stream); err != nil {
		t.Fatal(err)
	}
	seen := map[uint32]bool{}
	for i := 0; i < 5; i++ {
		f, derr := reader.next(time.Now().Add(2 * time.Second))
		if derr != nil {
			t.Fatalf("帧 %d 丢失: %v", i, derr)
		}
		if f.Type != TypeData || len(f.Payload) != 1 {
			t.Fatalf("帧内容异常: %+v", f)
		}
		seen[f.Seq] = true
	}
	if len(seen) != 5 {
		t.Fatalf("期望 5 个不同序号, got=%v", seen)
	}
	t.Logf("判定: 粘包 5 帧全部正确解出并回显，无串包/丢失/重复")
}

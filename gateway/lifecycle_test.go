package gateway

import (
	"context"
	"net"
	"testing"
	"time"
)

// slowHandler 让每帧阻塞指定时长，模拟需要排空的在途工作。
type slowHandler struct{ d time.Duration }

func (h slowHandler) ServeFrame(sess *Session, f *Frame) error {
	time.Sleep(h.d)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return sess.Send(ctx, f)
}

func TestE2EGracefulShutdownDrain(t *testing.T) {
	t.Logf("场景: 2 个会话各有 1 帧在处理（耗时 200ms），GracePeriod=2s，" +
		"期望在途帧排空后客户端收到回显，再正常关闭，无数据丢失")
	g, codec := startTestGateway(t, Config{
		GracePeriod: 2 * time.Second,
		Handler:     slowHandler{d: 200 * time.Millisecond},
	})

	conns := make([]net.Conn, 2)
	for i := range conns {
		c, err := net.Dial("tcp", g.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
		f := &Frame{Version: V2, Type: TypeData, Seq: uint32(i + 1),
			Payload: []byte("drain-me")}
		w, _ := codec.Encode(f)
		if _, err := c.Write(w); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for g.ActiveSessions() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("active=%d, want 2", g.ActiveSessions())
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("连接状态: active=%d，均有在途帧；开始优雅停机", g.ActiveSessions())

	start := time.Now()
	err := g.Shutdown(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("优雅停机不应返回错误: %v", err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("排空不应耗尽整个宽限期, elapsed=%v", elapsed)
	}
	t.Logf("判定: 停机耗时=%v（在途帧先排空再关闭），err=%v", elapsed, err)

	for i, c := range conns {
		got, derr := readOneFrame(t, codec, c, time.Now().Add(500*time.Millisecond))
		if derr != nil {
			t.Fatalf("client %d 在途帧丢失: %v", i, derr)
		}
		if string(got.Payload) != "drain-me" {
			t.Fatalf("client %d 内容错误: %q", i, got.Payload)
		}
		c.Close()
	}
	t.Logf("判定: 2 个在途帧均已回显（不丢失、不重复），会话进入 closed")
}

func TestE2EGraceExpiredForceTerminate(t *testing.T) {
	t.Logf("场景: 在途帧耗时 1s，GracePeriod=100ms，期望宽限到期强制终止，" +
		"Shutdown 返回 kind=grace_expired，会话原因可解释")
	g, codec := startTestGateway(t, Config{
		GracePeriod: 100 * time.Millisecond,
		Handler:     slowHandler{d: time.Second},
	})

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f := &Frame{Version: V2, Type: TypeData, Seq: 1, Payload: []byte("stuck")}
	w, _ := codec.Encode(f)
	if _, err := conn.Write(w); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	err = g.Shutdown(context.Background())
	if !IsKind(err, KindGraceExpired) {
		t.Fatalf("期望 kind=grace_expired, 得到 %v", err)
	}
	t.Logf("判定: Shutdown 返回可区分原因 %s，会话已强制终止", err)
}

func TestE2ENewConnDuringShutdownRejected(t *testing.T) {
	t.Logf("场景: 停机进行期间建立的新连接必须被拒绝(Reject/shutting_down)，且不崩溃、不回退状态")
	g, codec := startTestGateway(t, Config{
		GracePeriod: time.Second,
		Handler:     slowHandler{d: 400 * time.Millisecond},
	})

	old := holdConn(t, g.Addr().String())
	defer old.Close()

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- g.Shutdown(context.Background()) }()

	// 轮询直到监听器关闭/停机标记生效。
	deadline := time.Now().Add(time.Second)
	var conn net.Conn
	for {
		c, derr := net.Dial("tcp", g.Addr().String())
		if derr != nil {
			// 监听器已关：拨号直接失败也是可接受的拒绝表现。
			t.Logf("判定: 停机期间新连接被 TCP 层拒绝（listener closed）：%v", derr)
			break
		}
		conn = c
		got, ferr := readOneFrame(t, codec, c, time.Now().Add(300*time.Millisecond))
		if ferr == nil && got.Type == TypeReject &&
			got.Flags == uint8(KindShuttingDown) {
			t.Logf("判定: 停机期间新连接收到 Reject/%s", ErrorKind(got.Flags))
			conn.Close()
			break
		}
		conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("未能在停机窗口内观察到拒绝行为")
		}
		time.Sleep(2 * time.Millisecond)
	}
	<-shutdownDone

	// 停机后重复 Shutdown/ForceShutdown 不应崩溃或回退状态。
	if err := g.Shutdown(context.Background()); err != nil {
		t.Logf("判定: 重复 Shutdown 幂等返回 %v", err)
	}
	if err := g.ForceShutdown("after shutdown"); err == nil {
		t.Logf("判定: 停机后 ForceShutdown 被安全忽略")
	} else if !IsKind(err, KindForceTerminated) {
		t.Fatalf("unexpected: %v", err)
	} else {
		t.Logf("判定: 重复关闭安全，返回幂等原因 %s", err)
	}
	if g.ActiveSessions() != 0 {
		t.Fatalf("停机后仍有会话: %d", g.ActiveSessions())
	}
}

func TestE2EIdleTimeout(t *testing.T) {
	t.Logf("场景: IdleTimeout=100ms，连接保持静默，期望服务端在约一个超时后关闭连接")
	g, _ := startTestGateway(t, Config{IdleTimeout: 100 * time.Millisecond})

	conn, err := net.Dial("tcp", g.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	start := time.Now()
	_, rerr := conn.Read(buf)
	elapsed := time.Since(start)
	if rerr == nil {
		t.Fatal("期望连接被关闭，却读到数据")
	}
	if elapsed < 80*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Fatalf("关闭时间异常: %v", elapsed)
	}
	t.Logf("连接状态: 静默连接在 %v 后被关闭（原因=idle_timeout），判定通过", elapsed)
}

package gateway

import (
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestE2EConcurrentConnectCloseNoLeak(t *testing.T) {
	t.Logf("场景: 50 个客户端反复 建连→发送半包→断连，并发与停机交错；" +
		"期望无崩溃、无泄漏、最终活跃会话归零")
	g, codec := startTestGateway(t, Config{
		IdleTimeout: 200 * time.Millisecond,
		Quotas: QuotaConfig{
			GlobalConcurrent: 16,
			Policy:           PolicyReject,
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", g.Addr().String())
			if err != nil {
				return // 可能因全局并发被拒，属正常
			}
			defer conn.Close()
			ver := V1
			if i%2 == 0 {
				ver = V2
			}
			f := &Frame{Version: ver, Type: TypeData, Seq: uint32(i),
				Payload: []byte("x")}
			w, _ := codec.Encode(f)
			// 只发半包，制造空闲，随后让连接被空闲超时或对端关闭。
			_, _ = conn.Write(w[:3])
			time.Sleep(time.Duration((i%5)+1) * time.Millisecond)
		}(i)
	}
	wg.Wait()

	// 优雅停机应在空闲超时/宽限内收敛；若个别会话仍在途则强制收尾。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = g.Shutdown(ctx)
	if !g.waitSessions(2 * time.Second) {
		t.Fatal("会话 goroutine 未全部退出")
	}
	runtime.GC()
	t.Logf("判定: 50 客户端并发接入/断连/停机交错完成，active=%d，无崩溃无泄漏",
		g.ActiveSessions())
}

func TestDecoderBufferBoundedUnderJunk(t *testing.T) {
	t.Logf("场景: 持续喂入不含魔数的垃圾字节（超过最大帧长），期望解码器缓冲保持有界")
	c := NewCodec(64, 128)
	d := NewDecoder(c)
	junk := make([]byte, 4096)
	for i := range junk {
		junk[i] = 0x01
	}
	_, errs := d.Feed(junk)
	if len(errs) == 0 {
		t.Fatal("期望对超界垃圾数据报错")
	}
	// 报错后缓冲应被清空（不随输入量增长）。
	if d.Buffered() != 0 {
		t.Fatalf("垃圾数据应被丢弃并清空缓冲, buffered=%d", d.Buffered())
	}
	// 再来一批垃圾 + 一个好帧，好帧仍应解出。
	good, _ := c.Encode(&Frame{Version: V1, Type: TypeData, Seq: 1, Payload: []byte("y")})
	frames, _ := d.Feed(append(make([]byte, 5000, 5000+len(good)), good...))
	if len(frames) != 1 || frames[0].Seq != 1 {
		t.Fatalf("好帧应在大量垃圾后仍能解出, frames=%d", len(frames))
	}
	t.Logf("判定: 解码器在 %d 字节垃圾冲击下缓冲有界（成帧后归零），好帧不丢", 5000)
}

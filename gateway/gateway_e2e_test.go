package gateway

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func testLogger(t *testing.T) Logger {
	return NewLogger(os.Stderr)
}

// frameReader 在单条连接上持续读帧，Decoder 跨多次 Read 复用，
// 正确处理回显帧被 TCP 切片（半包）与一次返回多帧（粘包）的情况。
type frameReader struct {
	t     *testing.T
	c     *Codec
	conn  net.Conn
	dec   *Decoder
	buf   []byte
	queue []*Frame // 一次 Feed 解出的多帧在此排队，避免丢弃
}

func newFrameReader(t *testing.T, c *Codec, conn net.Conn) *frameReader {
	return &frameReader{t: t, c: c, conn: conn, dec: NewDecoder(c), buf: make([]byte, 512)}
}

// next 读取下一帧；deadline 为绝对时间。半包在内部 Decoder 中保留，
// 粘包解出的多余帧进入队列。
func (r *frameReader) next(deadline time.Time) (*Frame, *Error) {
	_ = r.conn.SetReadDeadline(deadline)
	for len(r.queue) == 0 {
		n, err := r.conn.Read(r.buf)
		if n > 0 {
			frames, errs := r.dec.Feed(r.buf[:n])
			r.queue = append(r.queue, frames...)
			if len(errs) > 0 {
				return nil, errs[0]
			}
		}
		if err != nil {
			return nil, newError(KindConnIO, "test", "client read failed", err)
		}
	}
	f := r.queue[0]
	r.queue = r.queue[1:]
	return f, nil
}

// readOneFrame 保留给只读单帧的简单场景；多帧读取请使用 frameReader。
func readOneFrame(t *testing.T, c *Codec, conn net.Conn, deadline time.Time) (*Frame, *Error) {
	return newFrameReader(t, c, conn).next(deadline)
}

func startTestGateway(t *testing.T, cfg Config) (*Gateway, *Codec) {
	cfg.ListenAddr = "127.0.0.1:0"
	if cfg.Logger == nil {
		cfg.Logger = testLogger(t)
	}
	if cfg.Handler == nil {
		cfg.Handler = EchoHandler{}
	}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.ListenAndServe(context.Background()) }()
	deadline := time.Now().Add(2 * time.Second)
	for g.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("gateway did not start")
		}
		time.Sleep(time.Millisecond)
	}
	return g, g.codec
}

func TestE2EEchoConcurrent(t *testing.T) {
	t.Logf("场景: %d 个并发客户端，V1/V2 混合帧、逐字节分片发送，期望全部原样回显", 12)
	g, codec := startTestGateway(t, Config{})
	defer g.ForceShutdown("test end")

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", g.Addr().String())
			if err != nil {
				t.Errorf("client %d dial: %v", i, err)
				return
			}
			defer conn.Close()

			ver := V1
			if i%2 == 0 {
				ver = V2
			}
			payload := fmt.Sprintf("client-%d-ping", i)
			f := &Frame{Version: ver, Type: TypeData, Seq: uint32(i + 1),
				Payload: []byte(payload)}
			wire, err := codec.Encode(f)
			if err != nil {
				t.Errorf("client %d encode: %v", i, err)
				return
			}
			// 逐字节写，制造半包。
			for _, b := range wire {
				if _, err := conn.Write([]byte{b}); err != nil {
					t.Errorf("client %d write: %v", i, err)
					return
				}
			}
			got, derr := readOneFrame(t, codec, conn, time.Now().Add(3*time.Second))
			if derr != nil {
				t.Errorf("client %d read reply: %v", i, derr)
				return
			}
			if string(got.Payload) != payload || got.Version != ver {
				t.Errorf("client %d echo mismatch: %+v", i, got)
				return
			}
			t.Logf("判定: client=%d version=%d state=echo-ok payload=%q", i, ver, payload)
		}(i)
	}
	wg.Wait()
	if g.ActiveSessions() == 0 {
		t.Logf("连接状态: 客户端均已断开, active=%d", g.ActiveSessions())
	}
}

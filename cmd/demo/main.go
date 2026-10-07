// Command demo 启动本地双向流会话网关，回显收到的帧，并在收到 END_STREAM
// 后回送 END_STREAM 完成双向半关闭。用于手工冒烟验证：
//
//	go run ./cmd/demo
//
// 启动后打印监听地址；可用任意 TCP 工具按帧协议发送字节（见 README）。
// 30 秒无活动后空闲超时，Ctrl+C 触发宽限排空停机。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/gateway"
	"github.com/lisama569/stream-session-gateway-go/pkg/mux"
	"github.com/lisama569/stream-session-gateway-go/pkg/session"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	maxConn := flag.Int("max-conn", 100, "max concurrent connections")
	globalBytes := flag.Int64("global-bytes", 64<<20, "global inflight/outbound bytes budget")
	maxStreams := flag.Int("max-streams", 64, "max active logical streams per connection (mux mode)")
	idle := flag.Duration("idle", 30*time.Second, "idle timeout")
	grace := flag.Duration("grace", 3*time.Second, "shutdown grace")
	muxMode := flag.Bool("mux", false, "enable one-connection-multi-stream mode")
	flag.Parse()

	opts := session.DefaultOptions()
	opts.IdleTimeout = *idle
	opts.GracePeriod = *grace

	// 单路模式处理器：整条连接一个处理通道。
	var singleHandler session.Handler = session.HandlerFunc(func(ctx context.Context, s *session.Session, f *frame.Frame) {
		log.Printf("recv id=%d stream=%d version=%d len=%d end=%v",
			s.ID(), f.Stream, f.Version, len(f.Payload), f.EndStream())
		if f.EndStream() {
			_ = s.CloseWrite()
			return
		}
		if err := s.Send(ctx, f); err != nil {
			log.Printf("send id=%d err=%v", s.ID(), err)
		}
	})

	cfg := gateway.Config{
		Listen:         *addr,
		MaxConnections: *maxConn,
		GlobalBytes:    *globalBytes,
		Session:        opts,
		ShutdownGrace:  *grace,
	}

	if *muxMode {
		// 多路模式：按帧头 StreamID 分派；同路保序、跨路并发互不阻塞。
		opts.Mux = muxDefaultConfig(*maxStreams, *grace)
		cfg.Session = opts
		cfg.StreamHandler = session.StreamHandlerFunc(func(ctx context.Context, st *session.Stream, f *frame.Frame) {
			log.Printf("recv stream=%d version=%d len=%d end=%d", st.ID(), f.Version, len(f.Payload), b2i(f.EndStream()))
			if f.EndStream() {
				// 单路独立半关闭，不影响同连接其它路。
				_ = st.CloseWrite(ctx)
				return
			}
			if err := st.Send(ctx, f); err != nil {
				log.Printf("send stream=%d err=%v", st.ID(), err)
			}
		})
	}

	g := gateway.New(singleHandler, cfg)
	g.SetLogger(log.Printf)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := g.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}
	log.Printf("gateway listening on %s (mux=%v max-conn=%d max-streams=%d global-bytes=%d idle=%s grace=%s)",
		g.ListenAddr(), *muxMode, *maxConn, *maxStreams, *globalBytes, *idle, *grace)

	<-ctx.Done()
	log.Printf("shutting down (grace=%s) ...", *grace)
	if err := g.Shutdown(*grace); err != nil {
		log.Printf("shutdown: %v", err)
		os.Exit(1)
	}
	log.Printf("gateway stopped cleanly; stats=%+v", g.Stats())
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// muxDefaultConfig 给出 demo 用的多路配置：路数有界、每路在途/待写有界、背压。
func muxDefaultConfig(maxStreams int, grace time.Duration) mux.Config {
	c := mux.DefaultConfig()
	c.MaxStreams = maxStreams
	c.GracePeriod = grace
	return c
}

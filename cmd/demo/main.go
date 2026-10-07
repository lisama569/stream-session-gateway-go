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
	idle := flag.Duration("idle", 30*time.Second, "idle timeout")
	grace := flag.Duration("grace", 3*time.Second, "shutdown grace")
	muxMode := flag.Bool("mux", false, "enable per-connection logical stream multiplexing")
	flag.Parse()

	opts := session.DefaultOptions()
	opts.IdleTimeout = *idle
	opts.GracePeriod = *grace

	var g *gateway.Gateway
	if *muxMode {
		mopts := mux.DefaultOptions()
		mopts.IdleTimeout = *idle
		mopts.GracePeriod = *grace
		g = gateway.NewMux(mux.HandlerFunc(func(ctx context.Context, st *mux.Stream, f *frame.Frame) {
			log.Printf("recv conn=%d stream=%d v=%d len=%d end=%v",
				st.Conn().ID(), f.Stream, f.Version, len(f.Payload), f.EndStream())
			if f.EndStream() {
				_ = st.CloseWrite()
				return
			}
			if err := st.Send(ctx, f); err != nil {
				if qe, ok := mux.IsQuotaError(err); ok {
					log.Printf("send quota rejected conn=%d stream=%d scope=%s dir=%s reason=%s",
						st.Conn().ID(), f.Stream, qe.Scope, qe.Direction, qe.Reason)
					return
				}
				log.Printf("send conn=%d stream=%d err=%v", st.Conn().ID(), f.Stream, err)
			}
		}), gateway.Config{
			Listen:         *addr,
			MaxConnections: *maxConn,
			GlobalBytes:    *globalBytes,
			Mux:            mopts,
			ShutdownGrace:  *grace,
		})
	} else {
		g = gateway.New(session.HandlerFunc(func(ctx context.Context, s *session.Session, f *frame.Frame) {
			log.Printf("recv id=%d stream=%d version=%d len=%d end=%v",
				s.ID(), f.Stream, f.Version, len(f.Payload), f.EndStream())
			if f.EndStream() {
				_ = s.CloseWrite()
				return
			}
			if err := s.Send(ctx, f); err != nil {
				log.Printf("send id=%d err=%v", s.ID(), err)
			}
		}), gateway.Config{
			Listen:         *addr,
			MaxConnections: *maxConn,
			GlobalBytes:    *globalBytes,
			Session:        opts,
			ShutdownGrace:  *grace,
		})
	}
	g.SetLogger(log.Printf)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := g.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "start: %v\n", err)
		os.Exit(1)
	}
	log.Printf("gateway listening on %s (max-conn=%d global-bytes=%d idle=%s grace=%s)",
		g.ListenAddr(), *maxConn, *globalBytes, *idle, *grace)

	<-ctx.Done()
	log.Printf("shutting down (grace=%s) ...", *grace)
	if err := g.Shutdown(*grace); err != nil {
		log.Printf("shutdown: %v", err)
		os.Exit(1)
	}
	log.Printf("gateway stopped cleanly; stats=%+v", g.Stats())
}

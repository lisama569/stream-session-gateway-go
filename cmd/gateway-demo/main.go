// Command gateway-demo 在本地启动会话网关，并用若干并发客户端验证：
// 多版本帧、分片发送（半包/粘包）、回显与优雅停机。
//
// 用法：
//
//	go run ./cmd/gateway-demo
//
// 可选参数：
//
//	-addr string   监听地址（默认 127.0.0.1:19090）
//	-clients int   演示客户端数量（默认 5）
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	gw "github.com/lisama569/stream-session-gateway-go/gateway"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "listen address")
	clients := flag.Int("clients", 5, "number of demo clients")
	flag.Parse()

	logger := gw.NewLogger(os.Stdout)
	g, err := gw.New(gw.Config{
		ListenAddr:  *addr,
		IdleTimeout: 30 * time.Second,
		GracePeriod: 3 * time.Second,
		Handler:     gw.EchoHandler{},
		Logger:      logger,
		Quotas: gw.QuotaConfig{
			GlobalConcurrent:      32,
			GlobalInflightBytes:   4 << 20,
			SessionInflightFrames: 64,
			SessionInflightBytes:  1 << 20,
			Policy:                gw.PolicyBlock,
		},
	})
	if err != nil {
		log.Fatalf("new gateway: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := g.ListenAndServe(ctx); err != nil {
			log.Printf("serve ended: %v", err)
		}
	}()
	// 等待监听就绪。
	for g.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	fmt.Printf("gateway listening at %s, spawning %d clients...\n", g.Addr(), *clients)

	codec := gw.NewCodec(0, 0)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := runClient(g.Addr().String(), codec, i); err != nil {
				log.Printf("client %d error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	fmt.Println("all clients done, graceful shutdown...")
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := g.Shutdown(sctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	fmt.Println("gateway stopped")
}

func runClient(addr string, codec *gw.Codec, id int) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ver := gw.V1
	if id%2 == 0 {
		ver = gw.V2
	}
	f := &gw.Frame{Version: ver, Type: gw.TypeData, Seq: uint32(id + 1),
		Payload: []byte(fmt.Sprintf("hello-from-%d", id))}
	wire, err := codec.Encode(f)
	if err != nil {
		return err
	}

	// 逐字节写，制造半包；网关必须缓存并成帧。
	for _, b := range wire {
		if _, err := conn.Write([]byte{b}); err != nil {
			return err
		}
		time.Sleep(time.Millisecond)
	}

	dec := gw.NewDecoder(codec)
	buf := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		n, rerr := conn.Read(buf)
		if n > 0 {
			frames, errs := dec.Feed(buf[:n])
			for _, e := range errs {
				log.Printf("client %d decode error: %v", id, e)
			}
			for _, got := range frames {
				fmt.Printf("client %d (v%d) echo: seq=%d payload=%q\n",
					id, ver, got.Seq, string(got.Payload))
				return nil
			}
		}
		if rerr != nil {
			return rerr
		}
	}
}

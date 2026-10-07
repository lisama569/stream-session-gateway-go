package mux

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// testDeps 构造可控的接缝：出站项收集到切片、连接关闭被记录。
type testEnv struct {
	mu       sync.Mutex
	out      []*outRecord
	outCond  *sync.Cond
	closed   bool
	halfConn int32
	forced   []string
	events   []evRecord
	evMu     sync.Mutex
}

type outRecord struct {
	buf     []byte
	endConn bool
}

type evRecord struct {
	id     uint32
	ev     StreamEvent
	detail string
}

// testLimits 可显式控制连接级配额（<=0 不限）。
type testLimits struct {
	globalBytes int64
	connFrames  int64
	connBytes   int64
	connOutB    int64
}

func newTestEnv() *testEnv {
	e := &testEnv{}
	e.outCond = sync.NewCond(&e.mu)
	return e
}

func (e *testEnv) outCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.out)
}

func (e *testEnv) waitOut(n int) bool {
	deadline := time.Now().Add(3 * time.Second)
	e.mu.Lock()
	for len(e.out) < n {
		if time.Now().After(deadline) {
			e.mu.Unlock()
			return false
		}
		e.outCond.Wait()
	}
	e.mu.Unlock()
	return true
}

func (e *testEnv) deps(cfg Config, l testLimits) Deps {
	return Deps{
		GlobalBytes:        quota.NewLimiter(l.globalBytes),
		ConnInflightFrames: quota.NewLimiter(l.connFrames),
		ConnInflightBytes:  quota.NewLimiter(l.connBytes),
		ConnOutboundBytes:  quota.NewLimiter(l.connOutB),
		StreamSlots:        quota.NewGate(cfg.MaxStreams),
		EnqueueOut: func(ctx context.Context, buf []byte, endConn bool, release func()) error {
			e.mu.Lock()
			if e.closed {
				e.mu.Unlock()
				if release != nil {
					release()
				}
				return ErrStreamClosed
			}
			cp := append([]byte(nil), buf...)
			e.out = append(e.out, &outRecord{buf: cp, endConn: endConn})
			e.outCond.Broadcast()
			e.mu.Unlock()
			// 出站项在“写出”后由这里释放（模拟写循环成功）。
			if release != nil {
				release()
			}
			return nil
		},
		ConnCloseWrite: func() error {
			atomic.AddInt32(&e.halfConn, 1)
			return nil
		},
		ForceConn: func(code uint16, text string) {
			e.mu.Lock()
			e.forced = append(e.forced, fmt.Sprintf("code=%d %s", code, text))
			e.mu.Unlock()
		},
		Logf: func(string, ...any) {},
		OnEvent: func(id uint32, ev StreamEvent, detail string) {
			e.evMu.Lock()
			e.events = append(e.events, evRecord{id, ev, detail})
			e.evMu.Unlock()
		},
	}
}

func (e *testEnv) hasEvent(id uint32, ev StreamEvent) bool {
	e.evMu.Lock()
	defer e.evMu.Unlock()
	for _, r := range e.events {
		if r.id == id && r.ev == ev {
			return true
		}
	}
	return false
}

func (e *testEnv) forceTexts() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.forced...)
}

func newTestMux(t *testing.T, h Handler, cfg Config) (*Mux, *testEnv) {
	t.Helper()
	e := newTestEnv()
	m := New(h, cfg, e.deps(cfg, testLimits{}))
	m.Start()
	return m, e
}

// decoded 构造一个指定流/序号的帧（用负载承载序号便于核对）。
func payloadFrame(id uint32, seq int, end bool) *frame.Frame {
	flags := uint8(0)
	if end {
		flags = frame.FlagEndStream
	}
	return &frame.Frame{Version: frame.V2, Flags: flags, Stream: id, Payload: []byte(fmt.Sprintf("s%d-%d", id, seq))}
}

func decodeOut(t *testing.T, buf []byte) *frame.Frame {
	t.Helper()
	dec := frame.NewDecoder(bytes.NewReader(buf))
	var f frame.Frame
	if err := dec.Next(&f); err != nil {
		t.Fatalf("decode out: %v", err)
	}
	return &f
}

// TestMultiStreamOrderAndConcurrency：多路并发处理；同一路严格保序，
// 不同路各自独立推进。
func TestMultiStreamOrderAndConcurrency(t *testing.T) {
	var mu sync.Mutex
	got := map[uint32][]string{}
	var maxConcurrent int64
	var cur int64

	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		c := atomic.AddInt64(&cur, 1)
		for {
			pk := atomic.LoadInt64(&maxConcurrent)
			if c <= pk || atomic.CompareAndSwapInt64(&maxConcurrent, pk, c) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond) // 放大并发窗口
		mu.Lock()
		got[st.ID()] = append(got[st.ID()], string(f.Payload))
		mu.Unlock()
		atomic.AddInt64(&cur, -1)
		_ = st.Send(ctx, f)
	})
	cfg := DefaultConfig()
	cfg.MaxStreams = 8
	m, _ := newTestMux(t, h, cfg)

	const streams = 4
	const perStream = 20
	// 交错投递：s1,s2,s3,s4,s1,...，验证路内仍严格有序。
	for seq := 0; seq < perStream; seq++ {
		for id := uint32(1); id <= streams; id++ {
			if _, err := m.Deliver(context.Background(), payloadFrame(id, seq, false)); err != nil {
				t.Fatalf("deliver s%d seq%d: %v", id, seq, err)
			}
		}
	}
	if !waitFor(5*time.Second, func() bool {
		mu.Lock()
		done := true
		for id := uint32(1); id <= streams; id++ {
			if len(got[id]) != perStream {
				done = false
			}
		}
		mu.Unlock()
		return done
	}) {
		mu.Lock()
		counts := map[uint32]int{}
		for id := uint32(1); id <= streams; id++ {
			counts[id] = len(got[id])
		}
		mu.Unlock()
		t.Fatalf("frames not all processed: %v", counts)
	}
	mu.Lock()
	for id := uint32(1); id <= streams; id++ {
		arr := got[id]
		if len(arr) != perStream {
			t.Fatalf("stream %d got %d frames want %d", id, len(arr), perStream)
		}
		for seq := 0; seq < perStream; seq++ {
			want := fmt.Sprintf("s%d-%d", id, seq)
			if arr[seq] != want {
				t.Fatalf("stream %d order broken at %d: got %s want %s", id, seq, arr[seq], want)
			}
		}
	}
	conc := atomic.LoadInt64(&maxConcurrent)
	mu.Unlock()
	if conc < 2 {
		t.Fatalf("streams must be processed concurrently, peak=%d", conc)
	}
	if m.Stats().Opened != streams {
		t.Fatalf("opened=%d want %d", m.Stats().Opened, streams)
	}
	t.Logf("场景=多路并发与路内保序 路数=%d 每路=%d 峰值并发=%d 判定依据=路内序号严格递增且跨路并发>1",
		streams, perStream, conc)
}

// waitFor 轮询条件直到成立或超时。
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// TestSlowStreamDoesNotBlockOthers：stream 1 的处理器永久卡住（占住自己的
// worker），stream 2 的帧仍能被独立 worker 处理并回写，跨路互不阻塞。
func TestSlowStreamDoesNotBlockOthers(t *testing.T) {
	releaseSlow := make(chan struct{})
	defer close(releaseSlow)
	slowInWorker := make(chan struct{})
	var slowOnce sync.Once
	var fastDone int64

	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if st.ID() == 1 {
			slowOnce.Do(func() { close(slowInWorker) })
			// 尊重 ctx：连接/流终止时解除，避免泄漏 goroutine。
			select {
			case <-releaseSlow:
			case <-ctx.Done():
			}
			return
		}
		if err := st.Send(ctx, f); err != nil {
			t.Errorf("fast stream send: %v", err)
		}
		atomic.AddInt64(&fastDone, 1)
	})
	cfg := DefaultConfig()
	cfg.MaxStreams = 8
	m, env := newTestMux(t, h, cfg)

	// 慢路第一帧占住其 worker。
	if _, err := m.Deliver(context.Background(), payloadFrame(1, 0, false)); err != nil {
		t.Fatal(err)
	}
	<-slowInWorker

	// 此时慢路 worker 已被占住，但读循环尚未阻塞；投递 5 个快路帧，
	// 它们由快路独立 worker 处理，不应受慢路影响。
	for seq := 0; seq < 5; seq++ {
		if _, err := m.Deliver(context.Background(), payloadFrame(2, seq, false)); err != nil {
			t.Fatalf("fast deliver %d: %v", seq, err)
		}
	}
	if !waitFor(3*time.Second, func() bool { return atomic.LoadInt64(&fastDone) == 5 }) {
		t.Fatalf("fast stream blocked by slow stream, done=%d", atomic.LoadInt64(&fastDone))
	}
	if !env.waitOut(5) {
		t.Fatal("fast stream outputs not written")
	}
	for i := 0; i < 5; i++ {
		got := decodeOut(t, env.out[i].buf)
		if got.Stream != 2 || string(got.Payload) != fmt.Sprintf("s2-%d", i) {
			t.Fatalf("fast out %d = stream %d %q", i, got.Stream, got.Payload)
		}
	}
	if m.ActiveStreams() != 2 {
		t.Fatalf("active=%d want 2", m.ActiveStreams())
	}
	t.Logf("场景=跨路互不阻塞 slow=1已卡住 fast=2回写5帧 活跃=%d 判定依据=慢路worker卡住时快路5帧仍按时处理回写, 强制连接=%v",
		m.ActiveStreams(), env.forceTexts())
}

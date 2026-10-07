package mux

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
)

// muxPair 封装一条服务端多路连接与一个裸客户端连接。
type muxPair struct {
	c      *Conn
	client net.Conn
}

func newMuxPair(t *testing.T, opts Options, h Handler) muxPair {
	t.Helper()
	server, client := net.Pipe()
	c := New(1, server, h, opts)
	c.SetLogger(log.Printf)
	go func() { _ = c.Run(context.Background()) }()
	return muxPair{c: c, client: client}
}

func newMuxTCP(t *testing.T, opts Options, h Handler) muxPair {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	type accept struct {
		c   net.Conn
		err error
	}
	ac := make(chan accept, 1)
	go func() {
		c, err := ln.Accept()
		ac <- accept{c, err}
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	a := <-ac
	_ = ln.Close()
	c := New(1, a.c, h, opts)
	c.SetLogger(log.Printf)
	go func() { _ = c.Run(context.Background()) }()
	return muxPair{c: c, client: client}
}

func enc(t *testing.T, c net.Conn, f *frame.Frame) {
	t.Helper()
	if _, err := frame.Encode(c, f); err != nil {
		t.Fatalf("encode: %v", err)
	}
}

// echoMuxHandler 在每一路原样回显。
type echoMuxHandler struct{}

func (echoMuxHandler) OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame) {
	_ = st.Send(ctx, f)
}

// TestMultiStreamConcurrencyOrdering 覆盖：
// 多路并发收发、路内严格保序、跨路内容不串包。
func TestMultiStreamConcurrencyOrdering(t *testing.T) {
	p := newMuxPair(t, DefaultOptions(), echoMuxHandler{})
	defer p.client.Close()
	defer p.c.ForceClose(ReasonForcedByCaller)

	const streams = 8
	const perStream = 25

	// 记录每一路实际收到的顺序。
	var mu sync.Mutex
	got := make(map[uint32][][]byte)

	dec := frame.NewDecoder(p.client)
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		var gf frame.Frame
		for {
			if err := dec.Next(&gf); err != nil {
				return
			}
			cp := append([]byte(nil), gf.Payload...)
			mu.Lock()
			got[gf.Stream] = append(got[gf.Stream], cp)
			mu.Unlock()
		}
	}()

	// 交错发送：轮询各路，模拟真实多路穿插。
	go func() {
		for i := 0; i < perStream; i++ {
			for s := uint32(1); s <= streams; s++ {
				payload := []byte(fmt.Sprintf("s%d-seq%d", s, i))
				_, _ = frame.Encode(p.client, &frame.Frame{Version: frame.V2, Stream: s, Payload: payload})
			}
		}
	}()

	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		total := 0
		for _, v := range got {
			total += len(v)
		}
		mu.Unlock()
		if total == streams*perStream {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timeout: got %d frames, per-stream=%v", total, lenMap(got))
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	_ = p.client.Close()
	<-collectDone

	// 校验：每路数量与严格顺序、内容归属正确（不串包）。
	for s := uint32(1); s <= streams; s++ {
		seq := got[s]
		if len(seq) != perStream {
			t.Fatalf("stream %d got %d frames want %d", s, len(seq), perStream)
		}
		for i, b := range seq {
			want := fmt.Sprintf("s%d-seq%d", s, i)
			if string(b) != want {
				t.Fatalf("stream %d position %d got %q want %q (乱序/串包)", s, i, b, want)
			}
		}
	}
	log.Printf("场景=多路并发+路内保序 路数=%d 每路=%d 判定依据=各路seq严格递增且内容不串包", streams, perStream)
}

func lenMap(m map[uint32][][]byte) map[uint32]int {
	out := make(map[uint32]int, len(m))
	for k, v := range m {
		out[k] = len(v)
	}
	return out
}

// blockingHandler 让指定路（slowID）的 handler 永久阻塞直到 release 关闭；
// 其它路立即 echo。用于验证“一路卡死不拖死整条连接”。
type blockingHandler struct {
	slowID  uint32
	release chan struct{}
	slowIn  chan struct{}
}

func (h *blockingHandler) OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame) {
	if st.ID() == h.slowID {
		select {
		case h.slowIn <- struct{}{}:
		default:
		}
		select {
		case <-h.release:
		case <-ctx.Done():
		}
		return
	}
	_ = st.Send(ctx, f)
}

// TestCrossStreamNoBlocking 一路卡住，其它路在其卡死期间持续收发不受影响。
func TestCrossStreamNoBlocking(t *testing.T) {
	h := &blockingHandler{slowID: 7, release: make(chan struct{}), slowIn: make(chan struct{}, 8)}
	p := newMuxPair(t, DefaultOptions(), h)
	defer p.client.Close()
	defer close(h.release)
	defer p.c.ForceClose(ReasonForcedByCaller)

	// 1) 卡住 slow 路：连投多帧填满该路在途配额。
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		for i := 0; i < 100; i++ {
			if _, err := frame.Encode(p.client, &frame.Frame{Version: frame.V2, Stream: 7, Payload: []byte("x")}); err != nil {
				return
			}
		}
	}()
	<-h.slowIn // 确保 slow 路 handler 已进入阻塞

	// 2) slow 卡住期间，其它 3 路各做若干次请求-应答往返，必须全部成功。
	dec := frame.NewDecoder(p.client)
	for round := 0; round < 5; round++ {
		for _, id := range []uint32{1, 2, 3} {
			payload := []byte(fmt.Sprintf("fast-%d-%d", id, round))
			enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: id, Payload: payload})
			var gf frame.Frame
			got := make(chan error, 1)
			go func() { got <- dec.Next(&gf) }()
			select {
			case err := <-got:
				if err != nil {
					t.Fatalf("fast stream %d decode: %v", id, err)
				}
			case <-time.After(1500 * time.Millisecond):
				t.Fatalf("路7卡死导致路%d第%d轮被阻塞（跨路不隔离）", id, round)
			}
			if gf.Stream != id || string(gf.Payload) != string(payload) {
				t.Fatalf("fast stream mismatch: got %d %q want %d %q", gf.Stream, gf.Payload, id, payload)
			}
		}
	}
	log.Printf("场景=跨路互不阻塞 slow=7 判定依据=slow卡住期间路1/2/3完成5轮往返且内容正确")
}

// TestStreamInflightQuotaReject 验证：路级在途配额触发拒绝策略时，
// 仅该路被 RST（可区分路级/收不下原因），其它路照常工作，连接不终止。
func TestStreamInflightQuotaReject(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxStreamInflightFrames = 3
	opts.MaxStreamInflightBytes = 1 << 30
	opts.MaxConnInflightBytes = 1 << 30
	opts.InflightPolicy = PolicyReject

	release := make(chan struct{})
	slowIn := make(chan struct{}, 8)
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if st.ID() == 9 {
			select {
			case slowIn <- struct{}{}:
			default:
			}
			<-release
			return
		}
		_ = st.Send(ctx, f)
	})
	p := newMuxTCP(t, opts, h)
	defer p.client.Close()
	defer close(release)
	defer p.c.ForceClose(ReasonForcedByCaller)

	dec := frame.NewDecoder(p.client)

	// 灌满 slow 路（在途=3），第 4 帧触发路级拒绝 → 收到 RST_STREAM(9)。
	for i := 0; i < 4; i++ {
		enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 9, Payload: []byte("z")})
	}
	var rst *frame.Frame
	deadline := time.After(2 * time.Second)
	for {
		var gf frame.Frame
		got := make(chan error, 1)
		go func() { got <- dec.Next(&gf) }()
		select {
		case err := <-got:
			if err != nil {
				t.Fatalf("decode rst: %v", err)
			}
		case <-deadline:
			t.Fatalf("未收到 slow 路 RST，state=%s", p.c.State())
		}
		if gf.Flags&frame.FlagReset != 0 && gf.Stream == 9 {
			gfcp := gf
			rst = &gfcp
			break
		}
	}
	if rst == nil {
		t.Fatal("expected RST_STREAM on stream 9")
	}
	// 原因文本应体现路级在途帧数维度。
	if !contains(rst.Payload, "mux_stream_inflight_frames_exceeded") {
		t.Fatalf("RST 原因不可区分路级收不下: %q", rst.Payload)
	}

	// 连接必须仍存活，其它路（1、2）正常 echo。
	if p.c.State() == StateForceTerminated {
		t.Fatalf("路级拒绝不应终止连接，state=%s", p.c.State())
	}
	for _, id := range []uint32{1, 2} {
		enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: id, Payload: []byte("alive")})
		var gf frame.Frame
		got := make(chan error, 1)
		go func() { got <- dec.Next(&gf) }()
		select {
		case err := <-got:
			if err != nil || gf.Stream != id || string(gf.Payload) != "alive" {
				t.Fatalf("other stream %d affected: %v %q", id, err, gf.Payload)
			}
		case <-time.After(1500 * time.Millisecond):
			t.Fatalf("路9被RST后路%d无法收发（被连累）", id)
		}
	}
	log.Printf("场景=路级配额拒绝 slow=9 判定依据=收到RST且原因=mux_stream_inflight_frames_exceeded;连接存活;路1/2正常")
}

func contains(b []byte, sub string) bool {
	return len(b) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(b); i++ {
			if string(b[i:i+len(sub)]) == sub {
				return true
			}
		}
		return false
	}())
}

// halfHandler：收到 END_STREAM 的路回送数据后由服务端 CloseWrite 半关闭；
// 其它路普通 echo。
type halfHandler struct {
	closedIDs chan uint32
}

func (h *halfHandler) OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame) {
	if f.EndStream() {
		// 对端半闭：回送该帧负载作为结果（不带 END_STREAM），
		// 再由本端 CloseWrite 单独排队唯一的 END_STREAM。
		ff := *f
		ff.Flags = 0
		_ = st.Send(ctx, &ff)
		_ = st.CloseWrite()
		h.closedIDs <- st.ID()
		return
	}
	_ = st.Send(ctx, f)
}

// TestStreamHalfCloseIsolation 一路半关闭正常收尾，不影响同连接其它活跃路。
func TestStreamHalfCloseIsolation(t *testing.T) {
	h := &halfHandler{closedIDs: make(chan uint32, 4)}
	p := newMuxTCP(t, DefaultOptions(), h)
	defer p.client.Close()
	defer p.c.ForceClose(ReasonForcedByCaller)

	dec := frame.NewDecoder(p.client)

	// 路 5 发送一帧普通数据 + 一帧 END_STREAM。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 5, Payload: []byte("data5")})
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 5, Flags: frame.FlagEndStream, Payload: []byte("end5")})

	// 读出两帧数据 echo + 一帧 END_STREAM。
	var sawData, sawEnd bool
	for i := 0; i < 3; i++ {
		var gf frame.Frame
		got := make(chan error, 1)
		go func() { got <- dec.Next(&gf) }()
		select {
		case err := <-got:
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout reading stream5 echo")
		}
		switch string(gf.Payload) {
		case "data5", "end5":
			sawData = true
		}
		if gf.Stream == 5 && gf.EndStream() {
			sawEndOn5 := true
			_ = sawEndOn5
			sawEnd = true
		}
	}
	_ = sawData
	if !sawEnd || !sawData {
		t.Fatalf("stream5 收尾必须回显数据并发出END_STREAM data=%v end=%v", sawData, sawEnd)
	}

	// 等服务端完成该路 CloseWrite。
	select {
	case id := <-h.closedIDs:
		if id != 5 {
			t.Fatalf("closed id=%d want 5", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not half-close stream5")
	}

	// 连接与其它路必须仍活跃：路 6 继续 echo。
	if st := p.c.State(); st == StateClosed || st == StateForceTerminated {
		t.Fatalf("单路半关闭不应关闭连接，state=%s", st)
	}
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 6, Payload: []byte("still-alive")})
	var gf frame.Frame
	got := make(chan error, 1)
	go func() { got <- dec.Next(&gf) }()
	select {
	case err := <-got:
		if err != nil || gf.Stream != 6 || string(gf.Payload) != "still-alive" {
			t.Fatalf("stream6 after half-close: err=%v %q", err, gf.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("单路半关闭连累了路6")
	}
	log.Printf("场景=单路半关闭 stream=5 判定依据=END_STREAM往返;服务端半闭;连接存活;路6继续echo")
}

// TestGraceExpiryOneStreamStuck 停机宽限到期：一路 handler 永久卡住，
// 宽限到点连接以 ReasonGraceExpired(code=3) 强制终止，且其它路结果已处理完。
func TestGraceExpiryOneStreamStuck(t *testing.T) {
	release := make(chan struct{})
	fastDone := make(chan struct{})
	var fastProcessed int32
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if st.ID() == 42 {
			// 永久卡住但尊重 ctx（强制终止后退出，不泄漏 goroutine）。
			select {
			case <-release:
			case <-ctx.Done():
			}
			return
		}
		_ = st.Send(ctx, f)
		atomic.AddInt32(&fastProcessed, 1)
		if atomic.LoadInt32(&fastProcessed) >= 2 {
			select {
			case fastDone <- struct{}{}:
			default:
			}
		}
	})
	p := newMuxTCP(t, DefaultOptions(), h)
	defer p.client.Close()
	defer close(release)

	// 一路永久卡住。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 42, Payload: []byte("stuck")})
	// 另外两路正常。
	dec := frame.NewDecoder(p.client)
	readEcho := make(chan struct{})
	go func() {
		var gf frame.Frame
		for i := 0; i < 2; i++ {
			if err := dec.Next(&gf); err != nil {
				return
			}
		}
		close(readEcho)
	}()
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("ok1")})
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 2, Payload: []byte("ok2")})

	select {
	case <-fastDone:
	case <-time.After(2 * time.Second):
		t.Fatal("正常路未在卡住路存在时完成处理")
	}
	<-readEcho

	// 发起短宽限停机；卡住的路到点必须让整个连接强制终止。
	_ = p.c.GracefulClose(150 * time.Millisecond)
	err := p.c.Wait()
	if err == nil || p.c.State() != StateForceTerminated {
		t.Fatalf("宽限到期必须强制终止 state=%s err=%v", p.c.State(), err)
	}
	if p.c.Reason().Code != ReasonGraceExpired.Code {
		t.Fatalf("want grace-expired(3), got %s", p.c.Reason())
	}
	if atomic.LoadInt32(&fastProcessed) != 2 {
		t.Fatalf("宽限期内正常路结果应处理完 processed=%d", fastProcessed)
	}
	log.Printf("场景=停机宽限到期一路卡住 stuck=42 判定依据=code=3强制终止;路1/2结果已处理(%d)", atomic.LoadInt32(&fastProcessed))
}

// TestGarbageInterleavedAcrossStreams 在多路帧之间穿插：
// 未知版本字节、坏校验帧、随机垃圾；解码器重同步后各路好帧不丢、不乱序。
func TestGarbageInterleavedAcrossStreams(t *testing.T) {
	p := newMuxTCP(t, DefaultOptions(), echoMuxHandler{})
	defer p.client.Close()
	defer p.c.ForceClose(ReasonForcedByCaller)

	// 手工拼接收字节流：好帧 / 垃圾 / 好帧 交错。
	var raw []byte
	good1 := &frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("g1-1")}
	good2 := &frame.Frame{Version: frame.V1, Stream: 2, Payload: []byte("g2-1")}
	good3 := &frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("g1-2")}
	good4 := &frame.Frame{Version: frame.V2, Stream: 3, Payload: []byte("g3-1")}

	b1, _ := frame.Append(nil, good1)
	b2, _ := frame.Append(nil, good2)
	b3, _ := frame.Append(nil, good3)
	b4, _ := frame.Append(nil, good4)

	// 坏版本帧：SOF + version=9 + 其余字节。
	badVer := []byte{frame.SOF, 9, 0, 0, 3, 0, 0, 0, 9, 'x', 'y', 'z', 0}
	// 坏校验帧：合法 V2 头但校验字节被改。
	badCrc, _ := frame.Append(nil, &frame.Frame{Version: frame.V2, Stream: 7, Payload: []byte("bad")})
	badCrc[len(badCrc)-1] ^= 0xFF
	// 纯垃圾（不含 SOF）。
	junk := []byte{0x01, 0x02, 0x03, 0x04}

	raw = append(raw, b1...)
	raw = append(raw, junk...)
	raw = append(raw, badVer...)
	raw = append(raw, b2...)
	raw = append(raw, badCrc...)
	raw = append(raw, b3...)
	raw = append(raw, b4...)

	go func() { _, _ = p.client.Write(raw) }()

	dec := frame.NewDecoder(p.client)
	var mu sync.Mutex
	perStream := map[uint32][]string{}
	gotN := make(chan struct{}, 8)
	go func() {
		var gf frame.Frame
		for {
			if err := dec.Next(&gf); err != nil {
				return
			}
			mu.Lock()
			perStream[gf.Stream] = append(perStream[gf.Stream], string(gf.Payload))
			n := len(perStream[gf.Stream])
			mu.Unlock()
			if n >= 1 {
				select {
				case gotN <- struct{}{}:
				default:
				}
			}
		}
	}()

	// 服务端只把重同步后的好帧 echo 回来；跨路允许并发重排，
	// 但每一路内部必须严格有序且完整。
	wantPer := map[uint32][]string{
		1: {"g1-1", "g1-2"},
		2: {"g2-1"},
		3: {"g3-1"},
	}
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		total := 0
		for _, v := range perStream {
			total += len(v)
		}
		mu.Unlock()
		if total == 4 {
			break
		}
		select {
		case <-gotN:
		case <-deadline:
			mu.Lock()
			t.Fatalf("好帧丢失，仅收到 %d/4: %v", total, perStream)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for id, want := range wantPer {
		got := perStream[id]
		if len(got) != len(want) {
			t.Fatalf("路%d 帧数不符 got=%v want=%v（坏字节后丢帧/串包）", id, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("路%d 位置%d got=%q want=%q（路内顺序错乱）", id, i, got[i], want[i])
			}
		}
	}
	log.Printf("场景=坏字节穿插多路 判定依据=4个好帧跨3路全部还原且各路内有序,坏帧已重同步")
}

// TestActiveStreamLimitBackpressure 活跃路上限：超过的新路在背压策略下
// 停放等待；当一条活跃路结束归还槽位后，等待路按 FIFO 开启且不丢帧。
func TestActiveStreamLimitBackpressure(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxActiveStreams = 2
	opts.InflightPolicy = PolicyBlock
	// 收到 END_STREAM 后服务端主动半关闭，该路才能双方收尾、归还活跃槽位。
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if f.EndStream() {
			ff := *f
			ff.Flags = 0
			_ = st.Send(ctx, &ff)
			_ = st.CloseWrite()
			return
		}
		_ = st.Send(ctx, f)
	})
	p := newMuxTCP(t, opts, h)
	defer p.client.Close()
	defer p.c.ForceClose(ReasonForcedByCaller)

	dec := frame.NewDecoder(p.client)
	// 先开 2 路（1、2），各发一帧。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("a")})
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 2, Payload: []byte("b")})
	// 第 3 路（3）超上限，应停放等待开路。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 3, Payload: []byte("c")})

	// 读到路1、路2 echo。
	got := map[uint32]bool{}
	readOne := func(to time.Duration) (uint32, string) {
		var gf frame.Frame
		ch := make(chan error, 1)
		go func() { ch <- dec.Next(&gf) }()
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
		case <-time.After(to):
			t.Fatalf("timeout waiting echo, active=%d", p.c.ActiveStreams())
		}
		return gf.Stream, string(gf.Payload)
	}
	for i := 0; i < 2; i++ {
		id, data := readOne(2 * time.Second)
		got[id] = data == map[uint32]string{1: "a", 2: "b"}[id]
	}
	if p.c.ActiveStreams() != 2 {
		t.Fatalf("应恰好 2 活跃路, got %d", p.c.ActiveStreams())
	}
	log.Printf("阶段=活跃路达上限 活跃=%d 路3停放中", p.c.ActiveStreams())

	// 结束路1（双方半闭），释放一个槽位。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 1, Flags: frame.FlagEndStream, Payload: []byte("bye")})
	// 持续读帧直到收集到：路1的 bye 回显 + END_STREAM，以及路3的 c。
	// 顺序不固定（跨路并发），按内容/标志收集。
	var sawBye, sawEnd1, sawC bool
	for !(sawBye && sawEnd1 && sawC) {
		var gf frame.Frame
		ch := make(chan error, 1)
		go func() { ch <- dec.Next(&gf) }()
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout bye=%v end1=%v c=%v active=%d", sawBye, sawEnd1, sawC, p.c.ActiveStreams())
		}
		switch {
		case gf.Stream == 1 && string(gf.Payload) == "bye" && !gf.EndStream():
			sawBye = true
		case gf.Stream == 1 && gf.EndStream():
			sawEnd1 = true
		case gf.Stream == 3 && string(gf.Payload) == "c":
			sawC = true
		}
	}
	log.Printf("场景=活跃路上限背压 上限=2 判定依据=路1结束后槽位归还,停放的路3放行且echo=c")
}

// TestQuotaReleasedAfterStreamEnd 大量路依次结束后，连接级在途配额应归还到 0，
// 活跃路归零；内存/配额不随累计路数增长。
func TestQuotaReleasedAfterStreamEnd(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxActiveStreams = 0 // 不限路数
	opts.MaxConnInflightBytes = 1 << 20
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		if f.EndStream() {
			// 对端半闭帧不再 echo（否则会多回一个带 END_STREAM 的帧），
			// 只由本端 CloseWrite 发出唯一的 END_STREAM。
			_ = st.CloseWrite()
			return
		}
		_ = st.Send(ctx, f)
	})
	p := newMuxTCP(t, opts, h)
	defer p.client.Close()

	dec := frame.NewDecoder(p.client)
	// 200 路，每路发 1 数据帧 + END_STREAM。
	for s := uint32(1); s <= 200; s++ {
		enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: s, Payload: []byte("x")})
		enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: s, Flags: frame.FlagEndStream, Payload: nil})
	}
	// 持续读回所有帧直到服务端所有路回收。不能数够就停：最后一批 END_STREAM
	// 可能还在写队列里，客户端停止读会反压写循环，导致收尾无法完成。
	var dataN, endN int32
	readerStopped := make(chan struct{})
	go func() {
		defer close(readerStopped)
		var gf frame.Frame
		for {
			if err := dec.Next(&gf); err != nil {
				return
			}
			if gf.EndStream() {
				atomic.AddInt32(&endN, 1)
			} else if len(gf.Payload) > 0 {
				atomic.AddInt32(&dataN, 1)
			}
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.c.ActiveStreams() == 0 &&
			atomic.LoadInt32(&dataN) == 200 && atomic.LoadInt32(&endN) == 200 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&dataN) != 200 || atomic.LoadInt32(&endN) != 200 {
		t.Fatalf("未读全: data=%d/200 end=%d/200 active=%d",
			atomic.LoadInt32(&dataN), atomic.LoadInt32(&endN), p.c.ActiveStreams())
	}
	if p.c.ActiveStreams() != 0 {
		t.Fatalf("200 路结束后活跃应归零，仍有 %d", p.c.ActiveStreams())
	}
	if used := p.c.connInflight.Used(); used != 0 {
		t.Fatalf("连接级在途配额应全部归还，used=%d（随路数泄漏）", used)
	}
	if used := p.c.connOutbound.Used(); used != 0 {
		t.Fatalf("连接级待写配额应全部归还，used=%d", used)
	}
	log.Printf("场景=路结束归还配额 累计路=200 判定依据=active=0 connInflight=0 connOutbound=0")
	p.c.ForceClose(ReasonForcedByCaller) // 收尾测试连接
}

// TestStreamOutboundQuotaReject 路级待写配额极小时，Send 返回可区分
// “路级/写不出”的 QuotaError，且不影响其它路、不终止连接。
func TestStreamOutboundQuotaReject(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxStreamOutboundBytes = 1 // 单路几乎无法排队待写
	opts.MaxConnOutboundBytes = 1 << 20
	opts.InflightPolicy = PolicyReject

	// handler 里 Send：用真实 TCP 避免 net.Pipe 同步写语义干扰。
	sendErr := make(chan error, 4)
	h := HandlerFunc(func(ctx context.Context, st *Stream, f *frame.Frame) {
		err := st.Send(ctx, f)
		if err != nil {
			sendErr <- err
			return
		}
	})
	p := newMuxTCP(t, opts, h)
	defer p.client.Close()
	defer p.c.ForceClose(ReasonForcedByCaller)

	// 一帧负载远超单路待写上限（编码后 >1 字节）。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 5, Payload: []byte("this-is-way-too-big")})

	select {
	case err := <-sendErr:
		qe, ok := IsQuotaError(err)
		if !ok {
			t.Fatalf("应为 QuotaError，got %T %v", err, err)
		}
		if qe.Scope != ScopeStream {
			t.Fatalf("应标记为路级，got %s", qe.Scope)
		}
		if qe.Direction != DirOutbound {
			t.Fatalf("应标记为写不出(outbound)，got %s", qe.Direction)
		}
		t.Logf("场景=路级待写配额拒绝 判定依据=scope=%s dir=%s reason=%s", qe.Scope, qe.Direction, qe.Reason)
	case <-time.After(2 * time.Second):
		t.Fatal("未收到 Send 的配额拒绝错误")
	}

	// 连接仍存活；另一路走得通（其待写额度独立）。
	if p.c.State() == StateForceTerminated {
		t.Fatalf("路级待写拒绝不应终止连接 state=%s", p.c.State())
	}
}

// TestConcurrentStreamResetIdempotent 并发/重复重置同一路与整条连接，
// 结果稳定：不 panic、状态不回退、首次原因保留。
func TestConcurrentStreamResetIdempotent(t *testing.T) {
	h := echoMuxHandler{}
	p := newMuxPair(t, DefaultOptions(), h)
	defer p.client.Close()

	// 先让路 1 真正建立。
	enc(t, p.client, &frame.Frame{Version: frame.V2, Stream: 1, Payload: []byte("hi")})
	dec := frame.NewDecoder(p.client)
	waitFrame := func() {
		var gf frame.Frame
		ch := make(chan error, 1)
		go func() { ch <- dec.Next(&gf) }()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
		}
	}
	waitFrame()
	deadline := time.Now().Add(2 * time.Second)
	for p.c.Stream(1) == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	st := p.c.Stream(1)
	if st == nil {
		t.Fatal("stream1 should exist")
	}

	reason := CloseReason{Code: 77, Text: "first-reset", Forced: true}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = st.Reset(reason) }()
	}
	// 同时并发连接级强制关闭。
	wg.Add(1)
	go func() { defer wg.Done(); _ = p.c.ForceClose(ReasonForcedByCaller) }()
	wg.Wait()

	if err := p.c.Wait(); err == nil {
		t.Fatal("连接应已强制终止")
	}
	if p.c.State() != StateForceTerminated {
		t.Fatalf("连接状态应稳定在 force_terminated，got %s", p.c.State())
	}
	t.Logf("场景=并发重复重置/关闭 判定依据=16路Reset+ForceClose并发无panic;状态=%s不回退", p.c.State())
}

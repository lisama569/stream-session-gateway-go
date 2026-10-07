// Package mux 在单条 frame 连接之上承载多路逻辑流（stream）：
// 按帧头 StreamID 分派到各自的串行处理路子，提供流级/连接级两级配额、
// 背压、独立半关闭与可解释的关闭原因。
//
// 设计要点：
//   - 读循环是唯一解码与唯一 jobCh 发送方；每路一个 worker 串行消费，
//     路内严格保序不串包，路间互不阻塞（一路卡住只阻塞自己的队列与读循环）。
//   - 两级配额：连接级（活跃路数、总在途帧/字节）与路级（每路在途帧/字节、
//     待写字节），外加网关全局字节预算；拒绝错误可区分 级×方向×原因。
//   - 关闭：流可独立半关闭/收尾；连接级宽限排空到期强制终止卡住的路；
//     强制路径立即归还全部配额，不等待无礼的处理器。
package mux

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// Policy 配额触发策略，与 session.Policy 数值保持一致。
type Policy uint8

const (
	PolicyReject Policy = 0
	PolicyBlock  Policy = 1
)

// StreamEvent 是观察者回调事件（测试与日志用）。
type StreamEvent string

const (
	EventOpen          StreamEvent = "open"
	EventRemoteHalf    StreamEvent = "remote_half"
	EventLocalHalf     StreamEvent = "local_half"
	EventClosed        StreamEvent = "closed"
	EventAborted       StreamEvent = "aborted"
	EventRejectInbound StreamEvent = "reject_inbound"
)

// Config 多路复用配置。
type Config struct {
	// MaxStreams 连接上同时存活（未收尾）的逻辑流上限；<=0 不限。
	MaxStreams int

	// 每一路的在途（已接收未处理完）帧/字节上限；<=0 不限。
	MaxStreamInflightFrames int
	MaxStreamInflightBytes  int64
	// MaxStreamOutboundBytes 每一路待写出队字节（编码后）上限；<=0 不限。
	MaxStreamOutboundBytes int64

	// Policy 配额触发时的策略。
	Policy Policy

	// GracePeriod 连接级排空时每一路的宽限；<=0 由使用方传入。
	GracePeriod time.Duration
}

// DefaultConfig 返回带界默认值：路数 64、每路在途 64 帧/1MiB、待写 1MiB、背压。
func DefaultConfig() Config {
	return Config{
		MaxStreams:              64,
		MaxStreamInflightFrames: 64,
		MaxStreamInflightBytes:  1 << 20,
		MaxStreamOutboundBytes:  1 << 20,
		Policy:                  PolicyBlock,
		GracePeriod:             5 * time.Second,
	}
}

// Stats 多路运行计数器快照。
type Stats struct {
	Opened          int64 // 累计开启的路数（不含控制流）
	ClosedNormally  int64 // 正常收尾路数
	Aborted         int64 // 异常终止路数
	RejectedStreams int64 // 流级拒收而丢弃的帧数（路在途/路数超限）
	ConnRejectedIn  int64 // 连接级在途拒收帧数
}

// StreamInfo 是某一路的可观测快照。
type StreamInfo struct {
	ID       uint32
	State    StreamState
	Inflight int64 // 在途字节
	Outbound int64 // 待写字节
}

// Handler 处理某一路上的帧；同一流的帧严格串行调用，不同流可并发。
type Handler interface {
	OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame)
}

// HandlerFunc 函数适配器。
type HandlerFunc func(ctx context.Context, st *Stream, f *frame.Frame)

func (fn HandlerFunc) OnStreamFrame(ctx context.Context, st *Stream, f *frame.Frame) {
	fn(ctx, st, f)
}

// 哨兵错误。
var (
	// ErrStreamClosed 流已收尾/终止后的操作。
	ErrStreamClosed = errors.New("mux: stream closed")
	// ErrStreamHalfClosed 流写侧已半关闭后再 Send。
	ErrStreamHalfClosed = errors.New("mux: stream write side half-closed")
	// ErrQuotaRejected 包装可区分维度与方向的配额拒绝。
	ErrQuotaRejected = errors.New("mux: quota rejected")
)

// Deps 是 Mux 与底层连接会话之间的接缝（由 session 实现/注入）。
type Deps struct {
	// GlobalBytes 网关级全局字节预算；nil 不记账。
	GlobalBytes *quota.Limiter
	// ConnInflightFrames / ConnInflightBytes 连接级入站在途配额。
	ConnInflightFrames *quota.Limiter
	ConnInflightBytes  *quota.Limiter
	// ConnOutboundBytes 连接级待写字节配额。
	ConnOutboundBytes *quota.Limiter
	// StreamSlots 连接级活跃路数闸门（容量=MaxStreams）。
	StreamSlots *quota.Gate

	// EnqueueOut 入队一个已编码待写项；release 在写出或丢弃时由消费方调用一次。
	EnqueueOut func(ctx context.Context, buf []byte, endConn bool, release func()) error
	// ConnCloseWrite 半关闭整个连接的写侧（控制流 END_STREAM）。
	ConnCloseWrite func() error
	// ForceConn 以连接级原因强制终止整条连接。
	ForceConn func(code uint16, text string)

	// Logf 结构化日志（nil 静默）。
	Logf func(format string, args ...any)
	// OnEvent 路事件观察者（可为 nil）。
	OnEvent func(id uint32, ev StreamEvent, detail string)
}

// Mux 是一条连接上的多路复用器。
type Mux struct {
	cfg     Config
	handler Handler
	deps    Deps

	mu      sync.Mutex
	streams map[uint32]*Stream // 存活（active/half）流；收尾后立即摘除
	intake  bool               // 输入是否已关闭（EOF/排空开始后不再接新帧）

	logf func(string, ...any)

	stats Stats // 原子计数
}

// New 创建复用器（不启动任何 goroutine）。
func New(h Handler, cfg Config, deps Deps) *Mux {
	if deps.Logf == nil {
		deps.Logf = func(string, ...any) {}
	}
	return &Mux{
		cfg:     cfg,
		handler: h,
		deps:    deps,
		streams: make(map[uint32]*Stream),
		logf:    deps.Logf,
	}
}

// ControlStreamID 返回控制流（stream 0）ID。
func ControlStreamID() uint32 { return 0 }

// Start 完成初始化：建立常驻控制流（stream 0），必须在首帧到达前调用一次。
func (m *Mux) Start() {
	cs := newStream(m, ControlStreamID(), true)
	m.mu.Lock()
	if _, exists := m.streams[cs.id]; !exists {
		m.streams[cs.id] = cs
	}
	m.mu.Unlock()
	cs.workerWG.Add(1)
	go cs.runWorker()
	m.logf("场景=多路复用器启动 maxStreams=%d policy=%d streamFrames=%d streamBytes=%d streamOut=%d",
		m.cfg.MaxStreams, m.cfg.Policy, m.cfg.MaxStreamInflightFrames,
		m.cfg.MaxStreamInflightBytes, m.cfg.MaxStreamOutboundBytes)
}

// logf 兜底，避免 deps 为零值时空指针。
func (m *Mux) log(format string, args ...any) { m.logf(format, args...) }

func (m *Mux) emit(id uint32, ev StreamEvent, detail string) {
	if m.deps.OnEvent != nil {
		m.deps.OnEvent(id, ev, detail)
	}
}

// slotGateGive 归还一个活跃路数槽位（控制流不占槽）。
func (m *Mux) slotGateGive() {
	if m.deps.StreamSlots != nil {
		m.deps.StreamSlots.Leave()
	}
}

// lookup 取存活流。
func (m *Mux) lookup(id uint32) (*Stream, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.streams[id]
	return s, ok
}

// Stats 返回计数器快照。
func (m *Mux) Stats() Stats {
	return Stats{
		Opened:          atomic.LoadInt64(&m.stats.Opened),
		ClosedNormally:  atomic.LoadInt64(&m.stats.ClosedNormally),
		Aborted:         atomic.LoadInt64(&m.stats.Aborted),
		RejectedStreams: atomic.LoadInt64(&m.stats.RejectedStreams),
		ConnRejectedIn:  atomic.LoadInt64(&m.stats.ConnRejectedIn),
	}
}

// ActiveStreams 返回存活（未进入吸收态）路数，不含常驻控制流。
func (m *Mux) ActiveStreams() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.streams {
		if !s.control {
			n++
		}
	}
	return n
}

// StreamInfo 返回某一路快照（存活期间可查；收尾后 ok=false）。
func (m *Mux) StreamInfo(id uint32) (StreamInfo, bool) {
	m.mu.Lock()
	s, ok := m.streams[id]
	m.mu.Unlock()
	if !ok {
		return StreamInfo{}, false
	}
	return s.snapshot(), true
}

// Streams 返回全部存活路快照（不含控制流）。
func (m *Mux) Streams() []StreamInfo {
	m.mu.Lock()
	out := make([]StreamInfo, 0, len(m.streams))
	for _, s := range m.streams {
		if !s.control {
			out = append(out, s.snapshot())
		}
	}
	m.mu.Unlock()
	return out
}

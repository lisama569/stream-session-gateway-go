package session

import "time"

const (
	PolicyReject uint8 = 0
	PolicyBlock  uint8 = 1
)

// Options 为单个会话的可配置项。零值配合 DefaultOptions 使用。
type Options struct {
	// NegotiatedVersion 为 0 表示同时接受 V1/V2；非 0 时收到其他版本帧
	// 将以协议错误终止（不静默丢弃）。
	NegotiatedVersion byte

	// MaxInflightFrames 已接收并派发、但尚未处理完成的帧数量上限。<=0 不限。
	MaxInflightFrames int
	// MaxInflightBytes 同上，按负载字节计权。<=0 不限。
	MaxInflightBytes int64
	// MaxOutboundBytes 待写出方向排队字节（编码后长度）上限。<=0 不限。
	MaxOutboundBytes int64
	// InflightPolicy 入站在途配额触发策略：PolicyReject / PolicyBlock。
	// 出站配额始终区分：PolicyReject 时 Send 立即返回可区分拒绝错误，
	// PolicyBlock 时 Send 对调用方施加背压。
	InflightPolicy uint8

	// IdleTimeout 任意方向无帧活动的超时；<=0 不启用。
	IdleTimeout time.Duration
	// GracePeriod GracefulClose 的宽限排空时长；<=0 时使用默认 5s。
	GracePeriod time.Duration
	// WriteTimeout 单次 conn.Write 超时；<=0 不设。
	WriteTimeout time.Duration

	// ConcurrentHandlers 为 true 时每帧在独立 goroutine 中处理（吞吐高，
	// 但应用层需自行保证顺序）；默认 false：同一会话严格按接收顺序串行
	// 处理，保证不串包、不乱序。
	ConcurrentHandlers bool
}

// DefaultOptions 返回带合理默认值的选项：
// 在途 1024 帧 / 4MiB，出站 2MiB，宽限 5s，入站背压。
func DefaultOptions() Options {
	return Options{
		NegotiatedVersion: 0,
		MaxInflightFrames: 1024,
		MaxInflightBytes:  4 << 20,
		MaxOutboundBytes:  2 << 20,
		InflightPolicy:    PolicyBlock,
		IdleTimeout:       0,
		GracePeriod:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
}

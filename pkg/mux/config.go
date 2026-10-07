package mux

import "time"

const (
	PolicyReject uint8 = 0
	PolicyBlock  uint8 = 1
)

// Options 为单条多路连接的可配置项。
type Options struct {
	// NegotiatedVersion 为 0 表示同时接受 V1/V2；非 0 时收到其他版本帧
	// 将以协议错误终止整条连接。
	NegotiatedVersion byte

	// MaxActiveStreams 连接上同时活跃（未终结）的逻辑路上限；<=0 不限。
	MaxActiveStreams int
	// MaxConnInflightBytes 连接级总在途负载字节上限（所有路合计）；<=0 不限。
	MaxConnInflightBytes int64
	// MaxConnOutboundBytes 连接级总待写（已编码）字节上限；<=0 不限。
	MaxConnOutboundBytes int64

	// MaxStreamInflightFrames 单路在途帧数上限；<=0 不限。
	MaxStreamInflightFrames int64
	// MaxStreamInflightBytes 单路在途负载字节上限；<=0 不限。
	MaxStreamInflightBytes int64
	// MaxStreamOutboundBytes 单路待写字节上限；<=0 不限。
	MaxStreamOutboundBytes int64

	// StreamQueueFrames 单路在分派层允许停放的在途帧数上限；<=0 用默认。
	// 它是“某一路不拖慢连接”的关键：停放达到上限后该路背压/拒绝，
	// 但分派器仍继续服务其它路。
	StreamQueueFrames int

	// InflightPolicy 配额触发策略：PolicyReject / PolicyBlock（默认）。
	InflightPolicy uint8

	// IdleTimeout 任意方向无帧活动的超时；<=0 不启用。
	IdleTimeout time.Duration
	// GracePeriod GracefulClose 的默认宽限；<=0 用 5s。
	GracePeriod time.Duration
	// WriteTimeout 单次 conn.Write 超时；<=0 不设。
	WriteTimeout time.Duration
}

// DefaultOptions 返回多路连接的默认配额：
// 活跃路 256；连接级在途 4MiB / 待写 2MiB；
// 单路在途 64 帧 / 256KiB、待写 256KiB、停放 64 帧；背压策略。
func DefaultOptions() Options {
	return Options{
		MaxActiveStreams:        256,
		MaxConnInflightBytes:    4 << 20,
		MaxConnOutboundBytes:    2 << 20,
		MaxStreamInflightFrames: 64,
		MaxStreamInflightBytes:  256 << 10,
		MaxStreamOutboundBytes:  256 << 10,
		StreamQueueFrames:       64,
		InflightPolicy:          PolicyBlock,
		IdleTimeout:             0,
		GracePeriod:             5 * time.Second,
		WriteTimeout:            10 * time.Second,
	}
}

package gateway

import (
	"io"
	"log"
	"time"
)

// Logger 是网关使用的最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// nopLogger 丢弃所有日志。
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// stdLogger 用标准库 log 实现 Logger。
type stdLogger struct{ l *log.Logger }

// NewLogger 把 io.Writer 包装为 Logger（nil 返回丢弃日志的 Logger）。
func NewLogger(w io.Writer) Logger {
	if w == nil {
		return nopLogger{}
	}
	return stdLogger{l: log.New(w, "[gateway] ", log.LstdFlags|log.Lmicroseconds)}
}

func (s stdLogger) Printf(format string, args ...any) {
	s.l.Printf(format, args...)
}

// QuotaConfig 描述两级配额与触发策略。
// 整型字段的语义：正数=该配额大小；0=显式不限制；负数=未设置（使用默认值）。
type QuotaConfig struct {
	GlobalConcurrent      int    // 全局最大并发会话数
	GlobalInflightBytes   int64  // 全局在途载荷字节数
	SessionInflightFrames int    // 单会话最大在途帧数
	SessionInflightBytes  int64  // 单会话最大在途载荷字节数
	Policy                Policy // 配额触发策略：拒绝或阻塞背压
}

// Config 是网关配置。
type Config struct {
	ListenAddr   string        // 本地监听地址，默认 "127.0.0.1:0"
	MaxPayloadV1 int           // V1 单帧载荷上限，默认 16KiB
	MaxPayloadV2 int           // V2 单帧载荷上限，默认 1MiB
	IdleTimeout  time.Duration // 读空闲超时；0 表示不做空闲超时
	GracePeriod  time.Duration // 优雅停机宽限排空时间；0 时使用默认 5s
	ReadBuffer   int           // 每次读 conn 的缓冲大小；0 时使用默认 4KiB
	Quotas       QuotaConfig
	Handler      Handler // 业务帧处理器，必填
	Logger       Logger  // nil 表示静默
}

// 配额的内部哨兵：<=0 表示不限。
const unlimited int64 = -1

const (
	defaultListenAddr = "127.0.0.1:0"
	defaultGrace      = 5 * time.Second
	defaultReadBuffer = 4 << 10
)

// defaultConfig 返回带默认值的配置。
func defaultConfig() Config {
	return Config{
		ListenAddr:   defaultListenAddr,
		MaxPayloadV1: defaultMaxPayloadV1,
		MaxPayloadV2: defaultMaxPayloadV2,
		IdleTimeout:  0,
		GracePeriod:  defaultGrace,
		ReadBuffer:   defaultReadBuffer,
		Quotas: QuotaConfig{
			// -1 表示未设置，合并时用下列真正的默认值替换。
			GlobalConcurrent:      -1,
			GlobalInflightBytes:   -1,
			SessionInflightFrames: -1,
			SessionInflightBytes:  -1,
			Policy:                PolicyDefault,
		},
		Logger: nopLogger{},
	}
}

func (c Config) validate() error {
	if c.Handler == nil {
		return newError(KindMalformedFrame, "config", "Handler is required", nil)
	}
	if c.MaxPayloadV1 <= 0 || c.MaxPayloadV2 <= 0 {
		return newError(KindMalformedFrame, "config",
			"payload limits must be positive", nil)
	}
	if c.MaxPayloadV1 > v1MaxWireLen {
		return newError(KindLengthExceeded, "config",
			"v1 payload limit exceeds protocol bound", nil)
	}
	if c.GracePeriod < 0 || c.IdleTimeout < 0 {
		return newError(KindMalformedFrame, "config",
			"durations must not be negative", nil)
	}
	if c.ReadBuffer < 0 {
		return newError(KindMalformedFrame, "config",
			"read buffer must not be negative", nil)
	}
	return nil
}

package gateway

import (
	"errors"
	"fmt"
)

// ErrorKind 是网关对外暴露的、可区分的错误类别。
//
// 设计原则：所有从外部（对端协议错误、网络错误、配额、生命周期）产生的
// 错误都会被归类为本表中的一种，绝不把底层 *net.OpError 等直接透出，
// 使调用方可以稳定地依据类别做判断与解释。
type ErrorKind int

const (
	// KindUnknown 是零值占位，不属于任何真实错误。
	KindUnknown          ErrorKind = iota
	KindUnknownVersion             // 帧版本号未知
	KindLengthExceeded             // 声明长度超过上限，或缓冲长期无法成帧
	KindChecksumMismatch           // 校验和不匹配
	KindMalformedFrame             // 帧结构无法解析（魔数丢失等）
	KindGlobalConcurrent           // 全局并发会话数超限
	KindGlobalBytes                // 全局在途字节数超限
	KindSessionFrames              // 单会话在途帧数超限
	KindSessionBytes               // 单会话在途字节数超限
	KindIdleTimeout                // 空闲读超时
	KindGraceExpired               // 宽限排空时间耗尽
	KindSessionClosed              // 会话已关闭，拒绝再写
	KindShuttingDown               // 网关停机中，拒绝新接入或新帧
	KindForceTerminated            // 会话被强制终止
	KindPayloadTooLarge            // 待发送载荷超过该版本上限
	KindConnIO                     // 底层连接 IO 错误（已归类包装，不直接透出）
)

// String 返回错误类别的可读名称，便于日志解释。
func (k ErrorKind) String() string {
	switch k {
	case KindUnknownVersion:
		return "unknown_version"
	case KindLengthExceeded:
		return "length_exceeded"
	case KindChecksumMismatch:
		return "checksum_mismatch"
	case KindMalformedFrame:
		return "malformed_frame"
	case KindGlobalConcurrent:
		return "global_concurrent_limit"
	case KindGlobalBytes:
		return "global_bytes_limit"
	case KindSessionFrames:
		return "session_frames_limit"
	case KindSessionBytes:
		return "session_bytes_limit"
	case KindIdleTimeout:
		return "idle_timeout"
	case KindGraceExpired:
		return "grace_expired"
	case KindSessionClosed:
		return "session_closed"
	case KindShuttingDown:
		return "shutting_down"
	case KindForceTerminated:
		return "force_terminated"
	case KindPayloadTooLarge:
		return "payload_too_large"
	case KindConnIO:
		return "conn_io"
	default:
		return "unknown"
	}
}

// Error 是网关返回的统一错误类型。底层错误不会被直接透出，
// 而是包装为带类别、操作、会话标识的可解释错误（Cause 仍可通过 Unwrap 访问）。
type Error struct {
	Kind      ErrorKind
	Op        string // 发生错误时的操作，例如 "decode"、"accept"
	SessionID string
	Message   string
	Cause     error
}

func (e *Error) Error() string {
	sid := e.SessionID
	if sid == "" {
		sid = "-"
	}
	if e.Cause != nil {
		return fmt.Sprintf("gateway: op=%s kind=%s session=%s: %s: %s",
			e.Op, e.Kind, sid, e.Message, e.Cause.Error())
	}
	return fmt.Sprintf("gateway: op=%s kind=%s session=%s: %s",
		e.Op, e.Kind, sid, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// AsError 尝试把任意错误还原为 *Error。
func AsError(err error) (*Error, bool) {
	var ge *Error
	if errors.As(err, &ge) {
		return ge, true
	}
	return nil, false
}

// IsKind 判断 err 是否属于指定错误类别。
func IsKind(err error, kind ErrorKind) bool {
	if ge, ok := AsError(err); ok {
		return ge.Kind == kind
	}
	return false
}

func newError(kind ErrorKind, op, msg string, cause error) *Error {
	return &Error{Kind: kind, Op: op, Message: msg, Cause: cause}
}

func newSessionError(kind ErrorKind, op, sessionID, msg string, cause error) *Error {
	return &Error{Kind: kind, Op: op, SessionID: sessionID, Message: msg, Cause: cause}
}

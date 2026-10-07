package mux

import (
	"errors"
	"fmt"

	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// Scope 标识配额维度属于哪一级。
type Scope uint8

const (
	ScopeStream Scope = iota + 1 // 路级：单路自身的在途/待写
	ScopeConn                    // 连接级：整连接总在途、存活路数
	ScopeGlobal                  // 网关级：全局字节预算
)

func (s Scope) String() string {
	switch s {
	case ScopeStream:
		return "stream"
	case ScopeConn:
		return "connection"
	case ScopeGlobal:
		return "global"
	default:
		return fmt.Sprintf("scope(%d)", int(s))
	}
}

// Direction 标识触发配额的方向。
type Direction uint8

const (
	DirRecv  Direction = iota + 1 // 收不下（在途配额）
	DirWrite                      // 写不出（待写配额）
)

func (d Direction) String() string {
	switch d {
	case DirRecv:
		return "recv"
	case DirWrite:
		return "write"
	default:
		return fmt.Sprintf("dir(%d)", int(d))
	}
}

// RejectError 携带可解释的多路配额拒绝细节，可被 errors.As 取出。
type RejectError struct {
	Scope    Scope
	Dir      Direction
	Reason   quota.Reason
	StreamID uint32
	Limit    int64
	Asked    int64
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("mux quota rejected: scope=%s dir=%s stream=%d reason=%s limit=%d asked=%d",
		e.Scope, e.Dir, e.StreamID, e.Reason, e.Limit, e.Asked)
}

// Unwrap 使 errors.Is(err, ErrQuotaRejected) 成立。
func (e *RejectError) Unwrap() error { return ErrQuotaRejected }

// AsReject 尝试提取多路配额拒绝细节。
func AsReject(err error) (*RejectError, bool) {
	var r *RejectError
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

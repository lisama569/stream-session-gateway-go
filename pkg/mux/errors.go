package mux

import (
	"errors"
	"fmt"

	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// 连接/流级哨兵错误。
var (
	ErrConnClosed    = errors.New("mux: connection closed")
	ErrStreamClosed  = errors.New("mux: stream closed")
	ErrHalfClosed    = errors.New("mux: write side half-closed")
	ErrQuotaRejected = errors.New("mux: quota rejected")
	ErrNoSuchStream  = errors.New("mux: no such stream")
)

// QuotaScope 标记配额维度归属。
type QuotaScope uint8

const (
	ScopeStream QuotaScope = iota // 路级
	ScopeConn                     // 连接级
	ScopeGlobal                   // 网关全局
)

func (s QuotaScope) String() string {
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

// QuotaDirection 标记是收不下还是写不出。
type QuotaDirection uint8

const (
	DirInbound  QuotaDirection = iota // 收不下
	DirOutbound                       // 写不出
)

func (d QuotaDirection) String() string {
	if d == DirOutbound {
		return "outbound"
	}
	return "inbound"
}

// QuotaError 携带可解释的多路配额拒绝细节：
// 维度（路级/连接级/全局）× 方向（收/写）× 底层 quota.Reason。
type QuotaError struct {
	Scope     QuotaScope
	Direction QuotaDirection
	Reason    quota.Reason
	Stream    uint32
	Limit     int64
	Asked     int64
}

func (e *QuotaError) Error() string {
	return fmt.Sprintf("mux quota rejected: scope=%s dir=%s stream=%d reason=%s limit=%d asked=%d",
		e.Scope, e.Direction, e.Stream, e.Reason, e.Limit, e.Asked)
}

// Unwrap 使 errors.Is(err, quota.ErrRejected) 与 errors.Is(err, ErrQuotaRejected)
// 均成立，兼容既有判定习惯。
func (e *QuotaError) Unwrap() error { return ErrQuotaRejected }

// IsQuotaError 提取多路配额错误细节。
func IsQuotaError(err error) (*QuotaError, bool) {
	var qe *QuotaError
	if errors.As(err, &qe) {
		return qe, true
	}
	return nil, false
}

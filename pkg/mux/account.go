package mux

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// atomicFlag 是一次性 CAS 标志，用于保证每帧/每项配额恰好归还一次。
type atomicFlag struct {
	v atomic.Uint32
}

func (f *atomicFlag) set() bool { return f.v.CompareAndSwap(0, 1) }
func (f *atomicFlag) get() bool { return f.v.Load() != 0 }

// takeOne 封装单个维度的申请：reject=true 立即失败，否则 FIFO 背压。
func takeOne(ctx context.Context, l *quota.Limiter, n int64, reject bool, r quota.Reason) error {
	if l == nil || l.Limit() <= 0 {
		return nil
	}
	if reject {
		if l.TryTake(n) {
			return nil
		}
		return quota.NewRejected(r, l.Limit(), n)
	}
	return l.Take(ctx, n)
}

// asRejected 取出底层 *quota.RejectedError（没有则 nil）。
func asRejected(err error) *quota.RejectedError {
	var r *quota.RejectedError
	if errors.As(err, &r) {
		return r
	}
	return nil
}

// rejectErr 把底层配额拒绝翻译成多路 QuotaError。
func rejectErr(scope QuotaScope, dir QuotaDirection, stream uint32, err error) *QuotaError {
	qe := &QuotaError{Scope: scope, Direction: dir, Stream: stream}
	var r *quota.RejectedError
	if errors.As(err, &r) {
		qe.Reason, qe.Limit, qe.Asked = r.Reason, r.Limit, r.Asked
	} else {
		qe.Reason = quota.Reason(err.Error())
	}
	return qe
}

// ---- 连接级在途配额（连接级/全局字节由分派器非阻塞申请，见 loops.go）----

func (c *Conn) releaseInboundConn(n int64) {
	c.connInflight.Give(n)
	if c.globalBytes != nil {
		c.globalBytes.Give(n)
	}
}

// ---- 待写配额（Stream.Send / RST / END_STREAM 编码后申请）----
//
// 申请顺序固定 全局 → 连接 → 路；st 为 nil 表示连接级帧（StreamID=0）。

func (c *Conn) acquireOutbound(ctx context.Context, st *Stream, n int64, reject bool) error {
	if c.globalBytes != nil && c.globalBytes.Limit() > 0 && n > c.globalBytes.Limit() {
		return rejectErr(ScopeGlobal, DirOutbound, streamIDOf(st),
			quota.NewRejected(quota.ReasonGlobalInflightBytes, c.globalBytes.Limit(), n))
	}
	if c.connOutbound.Limit() > 0 && n > c.connOutbound.Limit() {
		return rejectErr(ScopeConn, DirOutbound, streamIDOf(st),
			quota.NewRejected(quota.ReasonMuxConnOutboundBytes, c.connOutbound.Limit(), n))
	}
	if st != nil && st.outBytes.Limit() > 0 && n > st.outBytes.Limit() {
		return rejectErr(ScopeStream, DirOutbound, st.id,
			quota.NewRejected(quota.ReasonMuxStreamOutboundBytes, st.outBytes.Limit(), n))
	}
	if err := takeOne(ctx, c.globalBytes, n, reject, quota.ReasonGlobalInflightBytes); err != nil {
		if q := asRejected(err); q != nil {
			return rejectErr(ScopeGlobal, DirOutbound, streamIDOf(st), q)
		}
		return err
	}
	if err := takeOne(ctx, c.connOutbound, n, reject, quota.ReasonMuxConnOutboundBytes); err != nil {
		if c.globalBytes != nil {
			c.globalBytes.Give(n)
		}
		if q := asRejected(err); q != nil {
			return rejectErr(ScopeConn, DirOutbound, streamIDOf(st), q)
		}
		return err
	}
	if st != nil {
		if err := takeOne(ctx, st.outBytes, n, reject, quota.ReasonMuxStreamOutboundBytes); err != nil {
			c.connOutbound.Give(n)
			if c.globalBytes != nil {
				c.globalBytes.Give(n)
			}
			if q := asRejected(err); q != nil {
				return rejectErr(ScopeStream, DirOutbound, st.id, q)
			}
			return err
		}
	}
	return nil
}

// releaseOutbound 按申请的逆序归还待写配额。
func (c *Conn) releaseOutbound(st *Stream, n int64) {
	if st != nil {
		st.outBytes.Give(n)
	}
	c.connOutbound.Give(n)
	if c.globalBytes != nil {
		c.globalBytes.Give(n)
	}
}

func streamIDOf(st *Stream) uint32 {
	if st == nil {
		return 0
	}
	return st.id
}

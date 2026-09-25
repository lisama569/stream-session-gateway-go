// Package quota 提供每会话与全局两级、基于权重的并发/缓冲配额，
// 支持“拒绝”和“背压（阻塞等待）”两种策略，拒绝原因可被 errors.Is/As 区分。
package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Reason 标识配额被拒绝的具体原因。
type Reason string

const (
	ReasonSessionInflightFrames Reason = "session_inflight_frames_exceeded"
	ReasonSessionInflightBytes  Reason = "session_inflight_bytes_exceeded"
	ReasonSessionOutboundBytes  Reason = "session_outbound_bytes_exceeded"
	ReasonGlobalInflightBytes   Reason = "global_inflight_bytes_exceeded"
	ReasonGlobalConnections     Reason = "global_connections_exceeded"
)

// ErrRejected 是所有配额拒绝错误的哨兵，便于 errors.Is 统一判定。
var ErrRejected = errors.New("quota: rejected")

// RejectedError 携带可解释的拒绝原因与配额细节。
type RejectedError struct {
	Reason Reason
	Limit  int64
	Asked  int64
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("quota rejected: reason=%s limit=%d asked=%d", e.Reason, e.Limit, e.Asked)
}

func (e *RejectedError) Unwrap() error { return ErrRejected }

// NewRejected 构造拒绝错误。
func NewRejected(r Reason, limit, asked int64) *RejectedError {
	return &RejectedError{Reason: r, Limit: limit, Asked: asked}
}

// IsRejected 判断 err 是否为配额拒绝，并提取细节。
func IsRejected(err error) (*RejectedError, bool) {
	var r *RejectedError
	if errors.As(err, &r) {
		return r, true
	}
	return nil, false
}

// Policy 配额触发时的策略。
type Policy uint8

const (
	PolicyReject Policy = iota
	PolicyBlock
)

// Limiter 是单个维度的加权配额器：
// TryTake 立即失败，Take 按 FIFO 顺序阻塞等待（公平背压，防止饥饿）。
type Limiter struct {
	limit int64

	mu   sync.Mutex
	used int64
	head *waiter
	tail *waiter
}

type waiter struct {
	n         int64
	ch        chan struct{}
	cancelled bool
	next      *waiter
}

// NewLimiter 创建上限为 limit 的配额器；limit<=0 表示不限。
func NewLimiter(limit int64) *Limiter { return &Limiter{limit: limit} }

// Unlimited 返回是否不设上限。
func (l *Limiter) Unlimited() bool { return l.limit <= 0 }

// Used 返回当前已占用量快照。
func (l *Limiter) Used() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

// Waiting 返回当前 FIFO 队列中等待者数量快照（主要用于测试同步）。
func (l *Limiter) Waiting() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for w := l.head; w != nil; w = w.next {
		n++
	}
	return n
}

// Limit 返回配额上限。
func (l *Limiter) Limit() int64 { return l.limit }

// TryTake 非阻塞尝试占用 n，成功返回 true。
func (l *Limiter) TryTake(n int64) bool {
	if l.limit <= 0 {
		return true
	}
	if n < 0 {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head != nil || l.used+n > l.limit {
		return false
	}
	l.used += n
	return true
}

// Take 按背压语义占用 n：FIFO 阻塞直到成功或 ctx 结束。
// ctx 取消时从等待队列摘除（若尚未被授予），已授予的配额仍由调用方负责归还。
func (l *Limiter) Take(ctx context.Context, n int64) error {
	if l.limit <= 0 {
		return nil
	}
	if n < 0 {
		return errors.New("quota: negative take")
	}
	l.mu.Lock()
	if l.head == nil && l.used+n <= l.limit {
		l.used += n
		l.mu.Unlock()
		return nil
	}
	w := &waiter{n: n, ch: make(chan struct{})}
	if l.tail == nil {
		l.head, l.tail = w, w
	} else {
		l.tail.next = w
		l.tail = w
	}
	l.mu.Unlock()

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		w.cancelled = true
		// 若恰好在取消与加锁之间被授予，ch 随后会关闭；消费掉这次授予并回补。
		l.mu.Unlock()
		select {
		case <-w.ch:
			l.Give(n) // 取消竞态中拿到的配额立即归还
		default:
		}
		return ctx.Err()
	}
}

// Give 归还 n 并尽可能按 FIFO 唤醒后续等待者。
func (l *Limiter) Give(n int64) {
	if l.limit <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.used -= n
	if l.used < 0 {
		l.used = 0
	}
	for l.head != nil {
		w := l.head
		if w.cancelled {
			l.head = w.next
			if l.head == nil {
				l.tail = nil
			}
			continue
		}
		if l.used+w.n > l.limit {
			return // 队头都放不下，后续权重更无法保证公平，停止授予
		}
		l.used += w.n
		l.head = w.next
		if l.head == nil {
			l.tail = nil
		}
		close(w.ch)
	}
}

// Gate 是固定槽位的连接数闸门（无权重）；容量<=0 表示不限。
type Gate struct {
	ch chan struct{}
}

// NewGate 创建容量 n 的闸门。
func NewGate(n int) *Gate {
	if n <= 0 {
		return &Gate{}
	}
	return &Gate{ch: make(chan struct{}, n)}
}

// TryEnter 非阻塞尝试占一个槽位。
func (g *Gate) TryEnter() bool {
	if g.ch == nil {
		return true
	}
	select {
	case g.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// Leave 释放一个槽位。
func (g *Gate) Leave() {
	if g.ch == nil {
		return
	}
	<-g.ch
}

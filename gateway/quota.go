package gateway

import (
	"context"
	"strconv"
)

// Policy 是配额触发时的处理策略。
type Policy int

const (
	// PolicyDefault 表示未显式配置，使用默认策略（阻塞背压）。
	PolicyDefault Policy = -1
	// PolicyReject 立即拒绝，返回可区分类别的错误（不申请、不等待）。
	PolicyReject Policy = 0
	// PolicyBlock 阻塞等待配额释放，形成对上游的背压；
	// ctx 结束时失败且不占用配额。
	PolicyBlock Policy = 1
)

// waiter 表示一个阻塞等待配额的申请。
type waiter struct {
	n    int64
	ch   chan struct{}
	dead bool // 已因 ctx 取消而移出队列
}

// Limiter 是带名称与配额类别标记的加权信号量。
// 阻塞模式下按 FIFO 唤醒，避免后来者越过先到者。
type Limiter struct {
	name   string
	size   int64
	policy Policy
	kind   ErrorKind

	mu      chan struct{} // 容量 1 的 channel 作为互斥锁
	cur     int64
	waiters []*waiter
}

func newLimiter(name string, size int64, policy Policy, kind ErrorKind) *Limiter {
	return &Limiter{
		name:   name,
		size:   size,
		policy: policy,
		kind:   kind,
		mu:     make(chan struct{}, 1),
	}
}

func (l *Limiter) lock()   { l.mu <- struct{}{} }
func (l *Limiter) unlock() { <-l.mu }

// Acquire 申请 n 个单位。
//   - n 大于配额总量：该申请永远无法满足，按该限流器自己的类别拒绝
//     （例如单会话字节限流器会给出 KindSessionBytes，而非通用错误）；
//   - PolicyReject：当前可用不足立即返回该限流器对应类别的 *Error；
//   - PolicyBlock：排队等待，直到配额满足或 ctx 结束（取消不占用配额）。
func (l *Limiter) Acquire(ctx context.Context, n int64) error {
	if n > l.size {
		return newError(l.kind, "quota",
			"request "+strconv.FormatInt(n, 10)+" exceeds limiter "+l.name+
				" capacity "+strconv.FormatInt(l.size, 10), nil)
	}
	l.lock()
	if l.cur+n <= l.size {
		l.cur += n
		l.unlock()
		return nil
	}
	if l.policy == PolicyReject {
		err := newError(l.kind, "quota",
			"quota exhausted on limiter="+l.name, nil)
		l.unlock()
		return err
	}

	w := &waiter{n: n, ch: make(chan struct{})}
	l.waiters = append(l.waiters, w)
	l.unlock()

	select {
	case <-w.ch:
		return nil // grantLocked 已把 n 计入 cur
	case <-ctx.Done():
		l.removeWaiter(w)
		return newError(l.kind, "quota",
			"blocked acquire canceled: "+ctx.Err().Error(), ctx.Err())
	}
}

// removeWaiter 取消失约的等待者；若恰好在被授予后取消，则归还其配额。
func (l *Limiter) removeWaiter(w *waiter) {
	l.lock()
	for i, x := range l.waiters {
		if x == w {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			l.unlock()
			return
		}
	}
	// 已不在队列：grant 已授予，归还并继续唤醒后续等待者。
	l.cur -= w.n
	if l.cur < 0 {
		l.cur = 0
	}
	l.grantLocked()
	l.unlock()
}

// TryAcquire 非阻塞尝试申请，成功返回 true。
func (l *Limiter) TryAcquire(n int64) bool {
	if n > l.size {
		return false
	}
	l.lock()
	ok := l.cur+n <= l.size
	if ok {
		l.cur += n
	}
	l.unlock()
	return ok
}

// Release 归还 n 个单位，并按 FIFO 尝试唤醒队首等待者；
// 队首都放不下时立即停止，防止后来的小请求插队。
func (l *Limiter) Release(n int64) {
	l.lock()
	l.cur -= n
	if l.cur < 0 {
		l.cur = 0
	}
	l.grantLocked()
	l.unlock()
}

// grantLocked 在持锁状态下授予配额，调用者必须持锁。
func (l *Limiter) grantLocked() {
	for len(l.waiters) > 0 {
		w := l.waiters[0]
		if l.cur+w.n > l.size {
			return
		}
		l.cur += w.n
		l.waiters = l.waiters[1:]
		close(w.ch)
	}
}

// Current 返回当前已占用配额。
func (l *Limiter) Current() int64 {
	l.lock()
	v := l.cur
	l.unlock()
	return v
}

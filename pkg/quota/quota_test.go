package quota

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"
)

func TestTryTakeAndRejectReason(t *testing.T) {
	l := NewLimiter(10)
	if !l.TryTake(6) {
		t.Fatal("first take should pass")
	}
	if l.TryTake(5) {
		t.Fatal("over-limit take must be rejected")
	}
	err := NewRejected(ReasonSessionInflightBytes, 10, 5)
	var r *RejectedError
	if !errors.As(err, &r) || r.Reason != ReasonSessionInflightBytes {
		t.Fatal("rejected reason must be extractable")
	}
	if !errors.Is(err, ErrRejected) {
		t.Fatal("must satisfy errors.Is ErrRejected")
	}
	l.Give(6)
	if !l.TryTake(10) {
		t.Fatal("after give, take should pass")
	}
	log.Printf("场景=配额拒绝 limit=%d used=%d 判定依据=超额TryTake=false且RejectedError原因可As提取", 10, l.Used())
}

func TestBlockingFIFO(t *testing.T) {
	l := NewLimiter(10)
	ctx := context.Background()
	if err := l.Take(ctx, 8); err != nil {
		t.Fatal(err)
	}
	// 两个等待者，按严格的调用顺序入队（用 ready 栅栏保证 waiter1
	// 已在队列中后才启动 waiter2，排除 goroutine 调度抖动）。
	order := make(chan int, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = l.Take(ctx, 5)
		order <- 1
		l.Give(5)
	}()
	// 等待 waiter1 确定已进入 FIFO 队列后再启动 waiter2。
	for l.Waiting() != 1 {
		time.Sleep(time.Millisecond)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = l.Take(ctx, 5)
		order <- 2
		l.Give(5)
	}()
	for l.Waiting() != 2 {
		time.Sleep(time.Millisecond)
	}
	// 先释放 3：used 8->5，只够队头 waiter1(5)；waiter2 必须留在队中，
	// 由此严格判定“不会越过队头授予”。
	l.Give(3)
	select {
	case first := <-order:
		if first != 1 {
			t.Fatalf("FIFO violated: waiter %d woke first", first)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter1 never woke")
	}
	// waiter1 醒来后自行 Give(5)，会连锁唤醒 waiter2。
	select {
	case second := <-order:
		if second != 2 {
			t.Fatalf("FIFO violated: waiter %d woke second", second)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter2 never woke")
	}
	wg.Wait()
	log.Printf("场景=FIFO背压 状态=两个5权重等待者排队 判定依据=唤醒顺序严格为1,2")
}

func TestTakeCancelReturnsQuota(t *testing.T) {
	l := NewLimiter(10)
	_ = l.TryTake(10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Take(ctx, 4) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("take did not react to cancel")
	}
	l.Give(10)
	// 取消的等待者不应再被唤醒：现在新的 Take 必须成功。
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := l.Take(ctx2, 10); err != nil {
		t.Fatalf("quota leaked from cancelled waiter: %v (used=%d)", err, l.Used())
	}
	log.Printf("场景=等待取消 used=%d 判定依据=取消后新Take(10)成功，配额无泄漏", l.Used())
}

func TestGate(t *testing.T) {
	g := NewGate(2)
	if !g.TryEnter() || !g.TryEnter() {
		t.Fatal("two enters should fit")
	}
	if g.TryEnter() {
		t.Fatal("third enter must be rejected")
	}
	g.Leave()
	if !g.TryEnter() {
		t.Fatal("after leave enter should pass")
	}
	// 无限容量闸门
	u := NewGate(0)
	for i := 0; i < 100; i++ {
		if !u.TryEnter() {
			t.Fatal("unlimited gate must accept")
		}
	}
}

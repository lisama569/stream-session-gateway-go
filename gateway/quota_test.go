package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterReject(t *testing.T) {
	l := newLimiter("test", 2, PolicyReject, KindGlobalBytes)
	ctx := context.Background()
	if err := l.Acquire(ctx, 2); err != nil {
		t.Fatal(err)
	}
	err := l.Acquire(ctx, 1)
	if !IsKind(err, KindGlobalBytes) {
		t.Fatalf("want KindGlobalBytes, got %v", err)
	}
	if err := l.Acquire(ctx, 3); !IsKind(err, KindGlobalBytes) {
		t.Fatalf("want KindGlobalBytes for oversize request, got %v", err)
	}
	l.Release(2)
	if err := l.Acquire(ctx, 1); err != nil {
		t.Fatalf("want success after release, got %v", err)
	}
	l.Release(1)
}

func TestLimiterBlock(t *testing.T) {
	l := newLimiter("test", 1, PolicyBlock, KindSessionFrames)
	ctx := context.Background()
	if err := l.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() { got <- l.Acquire(ctx, 1) }()
	select {
	case <-got:
		t.Fatal("should block while quota exhausted")
	case <-time.After(30 * time.Millisecond):
	}
	l.Release(1)
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not woken after release")
	}

	// 此时配额由被唤醒的 goroutine 持有（cur=1）。
	// ctx 取消不应让等待者占用配额。
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if err := l.Acquire(cctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline error, got %v", err)
	}
	cancel()
	if l.Current() != 1 {
		t.Fatalf("quota leaked, cur=%d", l.Current())
	}
}

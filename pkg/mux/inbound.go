package mux

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// Deliver 把一个已解码、已通过校验的帧分派到对应逻辑流。
//
// 由连接读循环在解出一帧后调用（单 goroutine）。它按“路数闸门→全局→
// 连接在途→路在途”的固定顺序申请配额，再把任务交给该路队列。
//   - PolicyBlock：配额不足时在此阻塞背压；只阻塞读循环，其它路的 worker
//     与写循环不受影响。可被 ctx 取消（连接强制终止）或该路 abort 唤醒。
//   - PolicyReject：连接级不足 → 返回 *RejectError（Scope=Conn/Global），
//     由上层终止整条连接；路级不足 → 仅丢弃本帧并终止该路，其它路继续。
//
// 返回 connHalfClosed=true 仅发生在控制流（stream 0）END_STREAM，
// 表示连接级半关闭，由 session 执行 TCP FIN 语义。
func (m *Mux) Deliver(ctx context.Context, f *frame.Frame) (bool, error) {
	id := f.Stream
	end := f.EndStream()

	// 控制流：连接级信号，不占路数/路级配额，仅做记账交给常驻 worker。
	if id == ControlStreamID() {
		if end {
			// 仍把帧交给处理器观察，同时通知连接级半关闭。
			s, _ := m.lookup(ControlStreamID())
			if s != nil {
				m.enqueueControl(ctx, s, f)
			}
			return true, nil
		}
		s, _ := m.lookup(ControlStreamID())
		if s != nil {
			m.enqueueControl(ctx, s, f)
		}
		return false, nil
	}

	s, isNew, reject := m.obtainStream(ctx, id)
	if reject != nil {
		return false, reject
	}

	// 已存在但已收尾的流（极端竞态：收尾与新帧同 ID）：按拒收/丢弃处理。
	if s == nil {
		atomic.AddInt64(&m.stats.RejectedStreams, 1)
		return false, nil
	}
	if isNew {
		s.workerWG.Add(1)
		go s.runWorker()
		m.emit(id, EventOpen, "")
		m.logf("场景=逻辑流开启 stream=%d 活跃=%d", id, m.ActiveStreams())
	}

	weight := int64(len(f.Payload))
	if rerr := m.acquireInbound(ctx, s, 1, weight); rerr != nil {
		// 路级收不下：按拒收策略只终止该路、丢弃本帧，读循环继续处理其它路。
		// 连接级/全局级收不下：错误上抛，由 session 终止整条连接。
		if rj, ok := rerr.(*RejectError); ok && rj.Scope == ScopeStream {
			atomic.AddInt64(&m.stats.RejectedStreams, 1)
			m.emit(id, EventRejectInbound, rj.Error())
			s.abort("stream inbound quota: " + rj.Error())
			m.logf("场景=路级入站拒收并终止该路 stream=%d 错误=%v", id, rj)
			return false, nil
		}
		return false, rerr
	}

	// 对端半闭标记必须在任务入队“之前”置位：否则 worker 可能抢先执行该帧，
	// 处理器里的 CloseWrite 会把状态误判为 Active→LocalHalf，等标记到达变
	// BothHalf 时 worker 已休眠，导致该路无法收尾。
	if end {
		st := s.markRemoteHalf()
		m.logf("场景=收到路END_STREAM stream=%d state=%s", id, st)
	}

	fr := *f
	j := &job{fr: &fr, weight: weight}

	// 队列入队。正常情况仅读循环这一发送方；阻塞时用 stopCh/ctx 唤醒，
	// 保证某一路 abort/连接强杀不会把读循环永久挂住。
	select {
	case s.jobCh <- j:
	case <-s.stopCh:
		j.releaseOnce(m, s)
	case <-ctx.Done():
		j.releaseOnce(m, s)
		return false, ctx.Err()
	}
	return false, nil
}

// enqueueControl 处理控制流数据帧：受连接级在途配额约束（非路级），
// 由常驻控制 worker 串行执行。
func (m *Mux) enqueueControl(ctx context.Context, s *Stream, f *frame.Frame) {
	weight := int64(len(f.Payload))
	if err := m.acquireConnInbound(ctx, 1, weight); err != nil {
		m.logf("场景=控制流配额拒收 stream=0 err=%v", err)
		return
	}
	fr := *f
	j := &job{fr: &fr, weight: weight}
	select {
	case s.jobCh <- j:
	case <-s.stopCh:
		j.releaseOnce(m, s)
	case <-ctx.Done():
		j.releaseOnce(m, s)
	}
}

// obtainStream 取已存活流，或为新流占一个活跃路数槽位后创建。
// 返回的 *RejectError 只会是“连接级活跃路数超限”。
func (m *Mux) obtainStream(ctx context.Context, id uint32) (*Stream, bool, error) {
	m.mu.Lock()
	if m.intake {
		// 排空开始后不再建立新路：直接丢弃（读循环即将停止）。
		m.mu.Unlock()
		return nil, false, nil
	}
	if s, ok := m.streams[id]; ok {
		m.mu.Unlock()
		return s, false, nil
	}
	m.mu.Unlock()

	// 新路：先尝试路数闸门。阻塞模式下等待（受 ctx 终止唤醒），
	// 拒绝模式下立即返回可区分原因。
	gate := m.deps.StreamSlots
	if gate != nil {
		if m.cfg.Policy == PolicyReject {
			if !gate.TryEnter() {
				atomic.AddInt64(&m.stats.RejectedStreams, 1)
				return nil, false, &RejectError{
					Scope: ScopeConn, Dir: DirRecv,
					Reason: quota.ReasonConnActiveStreams, StreamID: id,
					Limit: int64(m.cfg.MaxStreams), Asked: 1,
				}
			}
		} else {
			// Gate 本身无阻塞原语：用带超时的自旋等待 + ctx 取消。
			if !enterBlocking(ctx, gate) {
				return nil, false, ctx.Err()
			}
		}
	}

	// 双重检查：加锁期间可能已被同 ID 的另一帧创建（单读循环下不会发生，
	// 但保留严谨性）。
	m.mu.Lock()
	if m.intake {
		m.mu.Unlock()
		if gate != nil {
			gate.Leave()
		}
		return nil, false, nil
	}
	if s, ok := m.streams[id]; ok {
		m.mu.Unlock()
		if gate != nil {
			gate.Leave()
		}
		return s, false, nil
	}
	s := newStream(m, id, false)
	m.streams[id] = s
	m.mu.Unlock()
	atomic.AddInt64(&m.stats.Opened, 1)
	return s, true, nil
}

// enterBlocking 以背压语义等待一个连接路数槽位。
// Gate 是容量信号量，这里在 ctx 结束前周期性重试；不泄漏（失败方不取槽）。
func enterBlocking(ctx context.Context, g *quota.Gate) bool {
	for {
		if g.TryEnter() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

// acquireInbound 按 全局→连接帧→连接字节→路帧→路字节 顺序申请入站配额。
// 任何一级失败均回滚已申请量，返回可区分级/方向的 *RejectError 或 ctx 错误。
func (m *Mux) acquireInbound(ctx context.Context, s *Stream, frames, bytes int64) error {
	reject := m.cfg.Policy == PolicyReject

	// 单帧超单路上限：任何策略都无法容纳，立即给出路级拒收原因。
	if lim := s.inFrames.Limit(); lim > 0 && frames > lim {
		return &RejectError{Scope: ScopeStream, Dir: DirRecv, Reason: quota.ReasonStreamInflightFrames, StreamID: s.id, Limit: lim, Asked: frames}
	}
	if lim := s.inBytes.Limit(); lim > 0 && bytes > lim {
		return &RejectError{Scope: ScopeStream, Dir: DirRecv, Reason: quota.ReasonStreamInflightBytes, StreamID: s.id, Limit: lim, Asked: bytes}
	}
	d := m.deps
	if lim := d.ConnInflightFrames; lim != nil && lim.Limit() > 0 && frames > lim.Limit() {
		return &RejectError{Scope: ScopeConn, Dir: DirRecv, Reason: quota.ReasonSessionInflightFrames, StreamID: s.id, Limit: lim.Limit(), Asked: frames}
	}
	if lim := d.ConnInflightBytes; lim != nil && lim.Limit() > 0 && bytes > lim.Limit() {
		return &RejectError{Scope: ScopeConn, Dir: DirRecv, Reason: quota.ReasonSessionInflightBytes, StreamID: s.id, Limit: lim.Limit(), Asked: bytes}
	}
	if g := d.GlobalBytes; g != nil && g.Limit() > 0 && bytes > g.Limit() {
		return &RejectError{Scope: ScopeGlobal, Dir: DirRecv, Reason: quota.ReasonGlobalInflightBytes, StreamID: s.id, Limit: g.Limit(), Asked: bytes}
	}

	try := func(l *quota.Limiter, n int64, r quota.Reason, scope Scope, isStream bool) error {
		if l == nil || l.Limit() <= 0 {
			return nil
		}
		if reject {
			if l.TryTake(n) {
				return nil
			}
			return &RejectError{Scope: scope, Dir: DirRecv, Reason: r, StreamID: s.id, Limit: l.Limit(), Asked: n}
		}
		return l.Take(ctx, n)
	}

	// 固定申请顺序：全局 → 连接 → 路；失败反向回滚。
	if err := try(d.GlobalBytes, bytes, quota.ReasonGlobalInflightBytes, ScopeGlobal, false); err != nil {
		return err
	}
	if err := try(d.ConnInflightFrames, frames, quota.ReasonSessionInflightFrames, ScopeConn, false); err != nil {
		d.GlobalBytes.Give(bytes)
		return err
	}
	if err := try(d.ConnInflightBytes, bytes, quota.ReasonSessionInflightBytes, ScopeConn, false); err != nil {
		if d.ConnInflightFrames != nil {
			d.ConnInflightFrames.Give(frames)
		}
		if d.GlobalBytes != nil {
			d.GlobalBytes.Give(bytes)
		}
		return err
	}
	if err := try(s.inFrames, frames, quota.ReasonStreamInflightFrames, ScopeStream, true); err != nil {
		m.giveConnInbound(frames, bytes)
		if d.GlobalBytes != nil {
			d.GlobalBytes.Give(bytes)
		}
		return err
	}
	if err := try(s.inBytes, bytes, quota.ReasonStreamInflightBytes, ScopeStream, true); err != nil {
		s.inFrames.Give(frames)
		m.giveConnInbound(frames, bytes)
		if d.GlobalBytes != nil {
			d.GlobalBytes.Give(bytes)
		}
		return err
	}
	return nil
}

// acquireConnInbound 仅申请连接级+全局入站配额（控制流用）。
func (m *Mux) acquireConnInbound(ctx context.Context, frames, bytes int64) error {
	d := m.deps
	reject := m.cfg.Policy == PolicyReject
	try := func(l *quota.Limiter, n int64, r quota.Reason, scope Scope) error {
		if l == nil || l.Limit() <= 0 {
			return nil
		}
		if reject {
			if l.TryTake(n) {
				return nil
			}
			return &RejectError{Scope: scope, Dir: DirRecv, Reason: r, StreamID: 0, Limit: l.Limit(), Asked: n}
		}
		return l.Take(ctx, n)
	}
	if err := try(d.GlobalBytes, bytes, quota.ReasonGlobalInflightBytes, ScopeGlobal); err != nil {
		return err
	}
	if err := try(d.ConnInflightFrames, frames, quota.ReasonSessionInflightFrames, ScopeConn); err != nil {
		if d.GlobalBytes != nil {
			d.GlobalBytes.Give(bytes)
		}
		return err
	}
	if err := try(d.ConnInflightBytes, bytes, quota.ReasonSessionInflightBytes, ScopeConn); err != nil {
		if d.ConnInflightFrames != nil {
			d.ConnInflightFrames.Give(frames)
		}
		if d.GlobalBytes != nil {
			d.GlobalBytes.Give(bytes)
		}
		return err
	}
	return nil
}

// BeginDrain 进入连接级排空：停止接受新帧/新路，随后读循环应停止投递。
// 幂等。deadline 仅用于日志解释，真正的截止由 WaitDrain 的调用方控制。
func (m *Mux) BeginDrain(deadline time.Time) {
	m.closeIntake()
	m.logf("场景=多路排空开始 deadline=%s 存活=%d", deadline.Format("15:04:05.000"), m.ActiveStreams())
}

// WaitDrain 等待所有流在 deadline 前排空；到期仍未进入吸收态的流被强制终止，
// 并返回这些“卡住流”的 ID 与原因，供连接级关闭原因解释。
// 控制流常驻、不参与卡住判定。
func (m *Mux) WaitDrain(deadline time.Time) []uint32 {
	// 用 reflect.Select 同时等待“任一路收尾”与“宽限到期”，全程不派生
	// goroutine（避免每次轮询泄漏等待者）。
	for {
		m.mu.Lock()
		live := make([]*Stream, 0, len(m.streams))
		for _, s := range m.streams {
			if !s.control {
				live = append(live, s)
			}
		}
		m.mu.Unlock()
		if len(live) == 0 {
			m.logf("场景=多路排空完成")
			return nil
		}

		cases := make([]reflect.SelectCase, 0, len(live)+1)
		pending := 0
		for _, s := range live {
			select {
			case <-s.done:
				// 已完成，不加入等待集合。
			default:
				pending++
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.done)})
			}
		}
		if pending == 0 {
			m.logf("场景=多路排空完成")
			return nil
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			return m.abortStuck(live, "grace drain expired")
		}
		timer := time.NewTimer(rem)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer.C)})
		chosen, _, _ := reflect.Select(cases)
		timer.Stop()
		if chosen == len(cases)-1 {
			// 宽限到点：终止仍存活的路。
			return m.abortStuck(live, "grace drain expired")
		}
		// 否则某一路收尾，循环重新计算存活集合。
	}
}

// abortStuck 强制终止仍存活的流，归还其队列占用的全部配额，
// 返回被终止的流 ID（卡住集合）。当前正在执行的任务若处理器忽略流 ctx，
// 其配额由连接最终关闭统一兜底（见 AbortAll），这里先释放队列与记账。
func (m *Mux) abortStuck(live []*Stream, reason string) []uint32 {
	var stuck []uint32
	for _, s := range live {
		select {
		case <-s.done:
			continue
		default:
		}
		stuck = append(stuck, s.id)
		if s.abort(reason) {
			m.drainQueueQuota(s)
		}
	}
	if len(stuck) > 0 {
		m.logf("场景=宽限到期强制终止卡住流 streams=%s", joinIDs(stuck))
	}
	return stuck
}

// drainQueueQuota 释放仍滞留（未被 worker 取走）在该路队列中的任务配额。
// 对 worker 已取走、正在执行的任务，其 releaseOnce 在处理器返回时成为
// 空操作之外的兜底：这里也代为释放，保证连接级记账立即归零（不多还）。
// drainQueueQuota 释放该路仍占用的全部入站配额：包括滞留队列与 worker 正在
// 执行（可能忽略 ctx 永不返回）的当前任务。所有归还都经 releaseOnce，
// worker 事后结束时不会重复归还。
func (m *Mux) drainQueueQuota(s *Stream) {
	// 1) 正在执行的任务：连接级记账立即回收，不等待无礼处理器。
	if j := s.currentJob.Load(); j != nil {
		j.releaseOnce(m, s)
	}
	// 2) 非阻塞地抽空当前队列缓冲；worker 因 stopCh 已退出，不会再取。
	for {
		select {
		case j, ok := <-s.jobCh:
			if !ok {
				return // 队列已关闭且为空
			}
			if j != nil {
				j.releaseOnce(m, s)
			}
		default:
			return
		}
	}
}

// AbortAll 连接级强制终止：终止所有路（含控制流）、释放滞留队列配额，
// 不等待忽略 ctx 的处理器；保证各级在途记账立即归还，内存随路数有界回收。
func (m *Mux) AbortAll(reason string) {
	m.mu.Lock()
	live := make([]*Stream, 0, len(m.streams))
	for _, s := range m.streams {
		live = append(live, s)
	}
	m.intake = true
	m.mu.Unlock()
	for _, s := range live {
		if s.abort(reason) {
			m.drainQueueQuota(s)
		}
	}
	m.mu.Lock()
	m.streams = make(map[uint32]*Stream)
	m.mu.Unlock()
	m.logf("场景=多路连接强制终止 streams=%d reason=%s", len(live), reason)
}

func joinIDs(ids []uint32) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", id)
	}
	return b.String()
}

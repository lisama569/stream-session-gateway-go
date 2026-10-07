package mux

import (
	"context"
	"sync"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// newStream 构造一路（尚未计入活跃、未启动 worker）。
func (c *Conn) newStream(id uint32) *Stream {
	ctx, cancel := context.WithCancel(c.runCtx)
	return &Stream{
		id:       id,
		conn:     c,
		state:    streamActive,
		ctx:      ctx,
		cancel:   cancel,
		inCh:     make(chan *inbound, c.opts.StreamQueueFrames),
		inFrames: quota.NewLimiter(c.opts.MaxStreamInflightFrames),
		inBytes:  quota.NewLimiter(c.opts.MaxStreamInflightBytes),
		outBytes: quota.NewLimiter(c.opts.MaxStreamOutboundBytes),
		done:     make(chan struct{}),
	}
}

// ---- 分派器 → 自身事件 ----

// kickN 通知分派器某路又有 n 帧处理完。非阻塞：evCh 满时边沿合并，
// 分派器在已排队的 kick 中会读取 processed 计数并重试，不丢推进机会。
func (c *Conn) kickN(id uint32, n int64) {
	select {
	case c.evCh <- streamEvent{kind: evKick, id: id, n: n}:
	default:
	}
}

func (c *Conn) kick(id uint32) { c.kickN(id, 0) }

// ---- 流状态访问（worker/外部 goroutine 读，分派器写迁移）----

func (st *Stream) isTerminalState() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.state.terminal()
}

func (st *Stream) getState() streamState {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.state
}

// peerHalfAndDrained 报告对端已 END_STREAM 且本路帧全部处理完（无在途/停放）。
// 仅分派器调用。关键：用 delivered==processed 计数，而非 inCh 长度——
// 否则 END_STREAM 帧尚在 worker 手中时会被误判排空而提前关闭。
func (st *Stream) peerHalfAndDrained() bool {
	return st.endSeen &&
		len(st.pending) == 0 && len(st.overflow) == 0 &&
		len(st.inCh) == 0 && st.delivered == st.processed
}

// maybeCloseStream 在双方都半闭且本路已排空时把路推进 closed。仅分派器调用。
func (c *Conn) maybeCloseStream(st *Stream) {
	if st.getState().terminal() {
		return
	}
	st.mu.Lock()
	both := st.localSent && st.remoteSeen
	onlyRemote := !st.localSent && st.remoteSeen
	if onlyRemote {
		st.state = streamRemoteHalf
	}
	st.mu.Unlock()
	if !both {
		// 仅对端半闭：本端仍可写，保持 RemoteHalf，等连接级 EOF/停机收尾。
		return
	}
	c.finishStream(st, CloseReason{Code: 1, Text: "stream end both sides"})
}

// finishStream 把一路推进到吸收态（closed），幂等。仅分派器调用。
func (c *Conn) finishStream(st *Stream, reason CloseReason) {
	if !st.markTerminal(false, reason) {
		return
	}
	c.logf("场景=逻辑路收尾 stream=%d state=%s reason=%s", st.id, st.getState(), reason)
	c.notifyStreamEnd(st, reason)
	// 关闭 inCh：worker 排空残余帧后退出并由 evEnd 回收路槽；
	// 未启动 worker 的路（开路前被收尾）由分派器内联回收，避免自投递阻塞。
	st.closeInOnce.Do(func() { close(st.inCh) })
	if !st.opened {
		c.reapStream(st.id)
	}
}

// resetPeer 处理“对端 RST / 本地策略拒绝”导致的单路重置：标记 reset、
// 释放停放帧、关闭 inCh。仅分派器调用。
func (c *Conn) resetPeer(id uint32, reason CloseReason) {
	st := c.streams[id]
	if st == nil {
		return
	}
	if !st.markTerminal(true, reason) {
		return
	}
	// 释放停放中尚未进 inCh 的帧（overflow 未持配额的也一并清空；不重复）。
	for _, in := range st.pending {
		in.releaseAll()
	}
	st.pending = nil
	for _, in := range st.overflow {
		in.releaseAll()
	}
	st.overflow = nil
	st.cancel()
	st.closeInOnce.Do(func() { close(st.inCh) })
	c.logf("场景=逻辑路重置 stream=%d reason=%s", id, reason)
	c.notifyStreamEnd(st, reason)
	if !st.opened {
		c.reapStream(id)
	}
}

// notifyStreamEnd 触发可选生命周期回调并关闭 done（各一次）。
func (c *Conn) notifyStreamEnd(st *Stream, reason CloseReason) {
	st.once.Do(func() { close(st.done) })
	if lh, ok := c.handler.(StreamLifecycle); ok {
		lh.OnStreamEnd(st, reason)
	}
}

// markTerminal 幂等地把路推进 closed/reset 吸收态；返回是否为本次首迁。
func (st *Stream) markTerminal(forced bool, reason CloseReason) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.state.terminal() {
		return false
	}
	if forced {
		st.state = streamReset
	} else {
		st.state = streamClosed
	}
	st.reason = reason
	st.cancel()
	return true
}

// reapStream 回收一个已终结路的活跃槽位。仅分派器调用。
func (c *Conn) reapStream(id uint32) {
	st := c.streams[id]
	if st == nil {
		return // 已回收（幂等）：可能被“未开路内联回收”与 worker evEnd 重复触发。
	}
	if st.opened {
		st.opened = false
		c.active--
		if c.active < 0 {
			c.active = 0
		}
		c.activeN.Store(int64(c.active))
	}
	// 从路由表摘除（tableMu 与外部 Stream() 查询互斥）。
	c.tableMu.Lock()
	delete(c.streams, id)
	c.tableMu.Unlock()
	c.logf("场景=逻辑路回收 stream=%d 活跃=%d", id, c.active)
}

// markLocalStreamHalf 写出某路 END_STREAM 成功后记录本端半闭（写循环调用）。
func (c *Conn) markLocalStreamHalf(id uint32) {
	st := c.lookupStream(id)
	if st == nil {
		return
	}
	st.mu.Lock()
	if st.state.terminal() {
		st.mu.Unlock()
		return
	}
	st.localSent = true
	st.sentEnd = true
	both := st.localSent && st.remoteSeen
	st.state = deriveStreamState(st.localSent, st.remoteSeen)
	st.mu.Unlock()
	// 双方都已 END_STREAM：交由分派器在本路排空后收尾。
	if both {
		c.kick(id)
	}
}

// deriveStreamState 按两个半闭方向位派生对外状态。
func deriveStreamState(localSent, remoteSeen bool) streamState {
	switch {
	case localSent && remoteSeen:
		// 双方半闭：保持一个非吸收展示态，真正 closed 由排空后 finish 推进。
		return streamLocalHalf
	case localSent:
		return streamLocalHalf
	case remoteSeen:
		return streamRemoteHalf
	default:
		return streamActive
	}
}

// onPeerConnectionHalf 收到 StreamID=0 END_STREAM：整条连接对端半关闭，
// 进入宽限排空（不影响各路已在途处理）。
func (c *Conn) onPeerConnectionHalf() {
	c.mu.Lock()
	st := c.state
	c.mu.Unlock()
	if st.terminal() || st == StateClosing {
		return
	}
	c.logf("场景=收到对端连接级END_STREAM state=%s", st)
	_ = c.GracefulClose(c.opts.GracePeriod)
}

// onPeerConnectionEOF 对端彻底关闭连接。这等价于对端在所有路上都不再发数据
// （隐式 remote-half）：标记残余路 remoteSeen，已排空的立即正常收尾，
// 仍有在途帧的等 worker 排空后由 kick → maybeCloseStream 收尾。
func (c *Conn) onPeerConnectionEOF() {
	c.logf("场景=对端EOF 分派器推进残余路收尾 活跃=%d", c.active)
	for _, st := range c.snapshotStreams() {
		// 先取 worker 已处理帧数快照，避免 END_STREAM 尚在手中时误判排空。
		st.processed = st.processedAck.Load()
		st.endSeen = true
		st.mu.Lock()
		terminal := st.state.terminal()
		if !terminal {
			st.remoteSeen = true
			st.state = deriveStreamState(st.localSent, st.remoteSeen)
		}
		both := st.localSent && st.remoteSeen
		st.mu.Unlock()
		if terminal {
			continue
		}
		// 本地发起、从未对端收到数据的路（无 worker）：直接收尾回收。
		if !st.opened {
			c.finishStream(st, CloseReason{Code: 1, Text: "never opened, conn EOF"})
			continue
		}
		if both && st.peerHalfAndDrained() {
			c.finishStream(st, CloseReason{Code: 1, Text: "stream end at conn EOF"})
		}
	}
	_ = c.GracefulClose(c.opts.GracePeriod)
}

func (c *Conn) snapshotStreams() []*Stream {
	out := make([]*Stream, 0, len(c.streams))
	for _, st := range c.streams {
		out = append(out, st)
	}
	return out
}

// handleResetReq 处理本地 Stream.Reset 请求（仅分派器）。
func (c *Conn) handleResetReq(ctx context.Context, req *resetReq) {
	_ = ctx
	st := c.streams[req.id]
	if st == nil {
		req.done <- ErrNoSuchStream
		return
	}
	if st.isTerminalState() {
		req.done <- nil
		return
	}
	if err := c.enqueueReset(req.id, req.reason, true); err != nil {
		req.done <- err
		return
	}
	c.resetPeer(req.id, req.reason)
	req.done <- nil
}

// enqueueReset 编码并排队一个 RST_STREAM 帧（best-effort，连接坏则忽略）。
// enqueueReset 编码并排队一个 RST_STREAM 控制帧。RST 属于连接级控制帧：
// 只占全局+连接级待写配额，不计被重置路的路级待写额度，
// 因此即便该路待写配额被占满也能把 RST 发出去。
func (c *Conn) enqueueReset(id uint32, reason CloseReason, _ bool) error {
	fr := &frame.Frame{Version: frame.V2, Flags: frame.FlagReset, Stream: id, Payload: []byte(reason.Text)}
	buf, err := frame.Append(nil, fr)
	if err != nil {
		return err
	}
	n := int64(len(buf))
	if err := c.acquireOutboundConnOnly(n); err != nil {
		return err
	}
	c.sendMu.Lock()
	if c.outClosed {
		c.sendMu.Unlock()
		c.releaseOutboundConnOnly(n)
		return ErrConnClosed
	}
	c.outCh <- &outItem{buf: buf, resetID: id}
	c.sendMu.Unlock()
	c.signalActivity()
	return nil
}

// acquireOutboundConnOnly 只申请全局+连接级待写配额（控制帧用）。
func (c *Conn) acquireOutboundConnOnly(n int64) error {
	ctx := context.Background()
	if c.globalBytes != nil && c.globalBytes.Limit() > 0 && n > c.globalBytes.Limit() {
		return rejectErr(ScopeGlobal, DirOutbound, 0,
			quota.NewRejected(quota.ReasonGlobalInflightBytes, c.globalBytes.Limit(), n))
	}
	if c.connOutbound.Limit() > 0 && n > c.connOutbound.Limit() {
		return rejectErr(ScopeConn, DirOutbound, 0,
			quota.NewRejected(quota.ReasonMuxConnOutboundBytes, c.connOutbound.Limit(), n))
	}
	if err := takeOne(ctx, c.globalBytes, n, true, quota.ReasonGlobalInflightBytes); err != nil {
		if q := asRejected(err); q != nil {
			return rejectErr(ScopeGlobal, DirOutbound, 0, q)
		}
		return err
	}
	if err := takeOne(ctx, c.connOutbound, n, true, quota.ReasonMuxConnOutboundBytes); err != nil {
		if c.globalBytes != nil {
			c.globalBytes.Give(n)
		}
		if q := asRejected(err); q != nil {
			return rejectErr(ScopeConn, DirOutbound, 0, q)
		}
		return err
	}
	return nil
}

func (c *Conn) releaseOutboundConnOnly(n int64) {
	c.connOutbound.Give(n)
	if c.globalBytes != nil {
		c.globalBytes.Give(n)
	}
}

// sweepStreamsOnForce 强制终止时释放分派器持有的全部停放/待开/未路由帧并回收路。
func (c *Conn) sweepStreamsOnForce() {
	for _, in := range c.pendingOpens {
		in.releaseAll()
	}
	c.pendingOpens = nil
	c.pendingOpenID = map[uint32]struct{}{}
	// 已读入 rawCh 但尚未路由的帧：释放其可能持有的配额。
drainRaw:
	for {
		select {
		case in := <-c.rawCh:
			in.releaseAll()
		default:
			break drainRaw
		}
	}
	forceReason := CloseReason{Code: 10, Text: "connection force terminated", Forced: true}
	ids := make([]uint32, 0, len(c.streams))
	for id, st := range c.streams {
		ids = append(ids, id)
		for _, in := range st.pending {
			in.releaseAll()
		}
		st.pending = nil
		for _, in := range st.overflow {
			in.releaseAll()
		}
		st.overflow = nil
		if st.markTerminal(true, forceReason) {
			c.notifyStreamEnd(st, forceReason)
		}
		st.closeInOnce.Do(func() { close(st.inCh) })
	}
	for _, id := range ids {
		delete(c.streams, id)
	}
	c.active = 0
	c.activeN.Store(0)
}

var _ = sync.Mutex{}
var _ context.Context

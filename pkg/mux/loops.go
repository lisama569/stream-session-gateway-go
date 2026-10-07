package mux

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/lisama569/stream-session-gateway-go/pkg/frame"
	"github.com/lisama569/stream-session-gateway-go/pkg/quota"
)

// maxProtocolErrors 是连续坏帧（已重同步）容忍上限。
const maxProtocolErrors = 8

// runLocked 启动读/分派/写/看门狗 goroutine，并在读循环结束后主导收尾。
func (c *Conn) runLocked(parent context.Context) {
	ctx, cancel := c.runCtx, c.runCancel
	stopParent := make(chan struct{})
	go func() {
		select {
		case <-parent.Done():
			cancel()
		case <-stopParent:
		}
	}()
	defer close(stopParent)

	c.wgIO.Add(3)
	go c.writeLoop(ctx)
	go c.dispatchLoop(ctx)
	go c.readLoop(ctx)
	if c.opts.IdleTimeout > 0 {
		c.wgIO.Add(1)
		go c.idleWatchdog(ctx)
	}

	// 读循环是连接的生命周期主线：它退出意味着 EOF / 故障 / 强制终止。
	<-c.readerExited()
	// 通知分派器：不再有新帧，进入排空（仍继续服务 worker 收尾事件）。
	c.signalStopDispatch()

	if c.isForced() {
		c.teardown(true)
		return
	}

	c.mu.Lock()
	closing := c.state == StateClosing
	graceDl := c.graceDl
	c.mu.Unlock()
	if !closing {
		// 兜底：读循环正常 EOF 但未经 peerEOF 分支进入 Closing（理论不会发生）。
		c.beginGraceAfterEOF()
		closing = true
		c.mu.Lock()
		graceDl = c.graceDl
		c.mu.Unlock()
	}

	// 宽限排空：等待所有路 worker 完成；到点仍卡住的路随连接强制终止。
	if closing {
		remaining := time.Until(graceDl)
		if remaining < 0 {
			remaining = 0
		}
		workersDone := make(chan struct{})
		go func() { c.wgWorkers.Wait(); close(workersDone) }()
		select {
		case <-workersDone:
		case <-time.After(remaining + 20*time.Millisecond):
			c.logf("场景=宽限到期仍有在途路 判定依据=worker未排空 grace=%s", remaining)
			c.forceClose(ReasonGraceExpired)
			c.teardown(true)
			return
		}
	}
	c.teardown(false)
}

// readerExited 在 readLoop 彻底结束时关闭。
func (c *Conn) readerExited() <-chan struct{} { return c.readerDone }

// beginGraceAfterEOF 在读到对端 EOF（非强制）后进入排空。
func (c *Conn) beginGraceAfterEOF() {
	grace := c.opts.GracePeriod
	_ = c.GracefulClose(grace)
}

// teardown 关闭分派器输入、等待 io goroutine 回收、排空/关闭出站并 finalize。
func (c *Conn) teardown(forced bool) {
	c.drainAndCloseOut()
	c.wgIO.Wait()
	c.finalize(forced)
}

// ---- 读循环 ----

func (c *Conn) readLoop(ctx context.Context) {
	defer c.wgIO.Done()
	defer close(c.readerDone)
	dec := frame.NewDecoder(c.conn)
	var f frame.Frame
	badStreak := 0
	for {
		if c.isForced() {
			return
		}
		err := dec.Next(&f)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// 对端彻底关闭：通知分派器把残余路推进收尾，连接随后排空。
				select {
				case c.peerEOF <- struct{}{}:
				default:
				}
				return
			case errors.Is(err, io.ErrUnexpectedEOF):
				c.readFaultf(CloseReason{Code: 11, Text: "truncated frame: " + err.Error(), Forced: true})
				return
			case errors.Is(err, frame.ErrUnknownVersion),
				errors.Is(err, frame.ErrLengthExceeded),
				errors.Is(err, frame.ErrChecksum):
				badStreak++
				c.logf("场景=坏帧已重同步 连续=%d/%d 错误=%v", badStreak, maxProtocolErrors, err)
				if badStreak >= maxProtocolErrors {
					c.readFaultf(CloseReason{Code: 11, Text: "too many corrupt frames: " + err.Error(), Forced: true})
					return
				}
				f = frame.Frame{}
				continue
			default:
				if ctx.Err() != nil || c.isForced() {
					return
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() && c.isClosing() {
					c.logf("场景=宽限读截止到达，停止接收 错误=%v", err)
					return
				}
				c.readFaultf(CloseReason{Code: 12, Text: "read failure: " + err.Error(), Forced: true})
				return
			}
		}
		badStreak = 0
		c.signalActivity()

		if c.opts.NegotiatedVersion != 0 && byte(f.Version) != c.opts.NegotiatedVersion {
			c.readFaultf(CloseReason{Code: 11,
				Text: "version mismatch frame", Forced: true})
			return
		}

		fr := f
		f = frame.Frame{}
		// 读循环只负责解码并交给分派器；配额判定/背压/按路停放全部在
		// 分派器完成，保证某一路满时读循环不会停摆而连累后续其它路的帧。
		in := &inbound{fr: &fr, weight: int64(len(fr.Payload)), conn: c}
		select {
		case c.rawCh <- in:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Conn) readFaultf(r CloseReason) {
	select {
	case c.readErr <- readFault{err: errors.New(r.Text)}:
	default:
	}
	c.forceClose(r)
}

// ---- 分派器（多路路由核心）----

func (c *Conn) dispatchLoop(ctx context.Context) {
	defer c.wgIO.Done()
	rawCh := c.rawCh
	draining := false
	// 周期性补偿节拍：始终运行。即便“最后一个 kick”被边沿合并或与 processed
	// 快照竞态丢失，也会定期重读所有活跃路的 worker 已处理计数，推进 overflow
	// 重试与“双方半闭且已排空”的收尾，杜绝路卡死不回收。
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		// 强制终止：释放全部停放帧/路并立即退出。
		if ctx.Err() != nil {
			c.sweepStreamsOnForce()
			return
		}

		// 正常排空：读侧已无新帧，且所有路 worker 都已结束（在途全部处理完）。
		if draining && c.active == 0 && len(c.rawCh) == 0 && len(c.pendingOpens) == 0 {
			return
		}

		// 补偿节拍始终就绪（draining 与否都可能漏掉最后一个 kick）。
		select {
		case <-ctx.Done():
			c.sweepStreamsOnForce()
			return
		case rf := <-c.readErr:
			_ = rf
			return // 读循环已 forceClose
		case req := <-c.rstCh:
			c.handleResetReq(ctx, req)
		case ev := <-c.evCh:
			c.handleEvent(ev)
			c.tryPendingOpens()
		case <-c.peerEOF:
			c.onPeerConnectionEOF()
			draining = true
			rawCh = nil
		case <-c.stopDisp:
			draining = true
			rawCh = nil
		case <-tick.C:
			c.reconcileStreams()
		case in := <-rawCh:
			if in == nil {
				draining = true
				rawCh = nil
				continue
			}
			c.routeFrame(ctx, in)
			c.tryPendingOpens()
		}
	}
}

// reconcileStreams 重读所有活跃路的 worker 已处理计数快照，补推进 overflow
// 与“双方半闭且已排空”的收尾。仅分派器调用。
func (c *Conn) reconcileStreams() {
	for _, st := range c.snapshotStreams() {
		st.processed = st.processedAck.Load()
		if len(st.overflow) > 0 {
			c.pumpStream(st)
			continue
		}
		if st.endSeen && st.peerHalfAndDrained() {
			c.maybeCloseStream(st)
		}
	}
}

func (c *Conn) handleEvent(ev streamEvent) {
	if ev.kind == evEnd {
		c.reapStream(ev.id)
		return
	}
	if st := c.streams[ev.id]; st != nil {
		// 从 worker 的原子计数取快照（容忍 kick 边沿合并：不会漏计处理帧数）。
		st.processed = st.processedAck.Load()
		c.pumpStream(st)
	}
}

// signalStopDispatch 幂等关闭 stopDisp。
func (c *Conn) signalStopDispatch() {
	c.stopDispOnce.Do(func() { close(c.stopDisp) })
}

// routeFrame 把一帧按 StreamID 路由到对应路；含 RST/连接级控制判定。
func (c *Conn) routeFrame(ctx context.Context, in *inbound) {
	fr := in.fr
	// RST_STREAM：立即重置目标路（不占路配额、不入队）。
	if fr.Flags&frame.FlagReset != 0 {
		c.logf("场景=收到对端RST stream=%d len=%d", fr.Stream, len(fr.Payload))
		c.resetPeer(fr.Stream, CloseReason{Code: 20, Text: "peer RST: " + string(fr.Payload), Forced: true})
		in.releaseAll()
		return
	}
	// StreamID=0 的 END_STREAM：整条连接对端半关闭。
	if fr.Stream == 0 {
		if fr.EndStream() {
			c.onPeerConnectionHalf()
		} else {
			c.forceClose(CloseReason{Code: 23, Text: "data frame on stream 0", Forced: true})
		}
		in.releaseAll()
		return
	}
	st := c.streams[fr.Stream]
	if st == nil {
		if _, seen := c.seenIDs[fr.Stream]; seen {
			// 已终结路上的迟到帧：对端可能漏收 RST，回一个 RST 并丢弃，不动其它路。
			in.releaseAll()
			_ = c.enqueueReset(fr.Stream, ReasonStreamReset, false)
			return
		}
		// 首帧：创建路由条目（尚未计入活跃/未启动 worker）。
		st = c.newStream(fr.Stream)
		c.tableMu.Lock()
		c.streams[fr.Stream] = st
		c.tableMu.Unlock()
		c.seenIDs[fr.Stream] = struct{}{}
	}
	if st.opened {
		c.deliverOrPark(st, in)
		return
	}
	// 等待“开路”的帧：受活跃路上限约束。
	if c.activeStreamsLimit() <= 0 || c.active < c.activeStreamsLimit() {
		c.openStream(st)
		c.deliverOrPark(st, in)
		return
	}
	if c.opts.InflightPolicy == PolicyReject {
		in.releaseAll()
		_ = c.enqueueReset(fr.Stream, ReasonTooManyStreams, false)
		c.logf("场景=活跃路上限拒绝 stream=%d 活跃=%d 判定依据=reject策略", fr.Stream, c.active)
		return
	}
	// 背压：停放等待开路，期间其它路照常路由。
	if _, ok := c.pendingOpenID[fr.Stream]; !ok {
		c.pendingOpenID[fr.Stream] = struct{}{}
	}
	c.pendingOpens = append(c.pendingOpens, in)
}

// activeStreamsLimit 返回活跃路上限；<=0 不限。
func (c *Conn) activeStreamsLimit() int { return c.opts.MaxActiveStreams }

// deliverOrPark 对一帧做三级非阻塞判定：路级在途配额 → 连接级总在途配额。
// 任一维度暂不可得（背压策略）时把帧停放到该路 overflow 并立即返回，
// 分派器继续服务其它路；拒绝策略则按维度 RST（路级）或终止（连接级）。
func (c *Conn) deliverOrPark(st *Stream, in *inbound) {
	if st.isTerminalState() {
		in.releaseAll()
		return
	}
	reject := c.opts.InflightPolicy == PolicyReject

	// 1) 路级在途配额（帧数 + 字节），非阻塞；只在尚未持有时申请。
	in.mu.Lock()
	needStream := !in.heldFr
	in.mu.Unlock()
	if needStream {
		if !st.inFrames.Unlimited() && 1 > st.inFrames.Limit() {
			c.rejectOrOverflow(st, in, reject,
				rejectErr(ScopeStream, DirInbound, st.id,
					quota.NewRejected(quota.ReasonMuxStreamInflightFrames, st.inFrames.Limit(), 1)))
			return
		}
		if !st.inBytes.Unlimited() && in.weight > st.inBytes.Limit() {
			c.rejectOrOverflow(st, in, reject,
				rejectErr(ScopeStream, DirInbound, st.id,
					quota.NewRejected(quota.ReasonMuxStreamInflightBytes, st.inBytes.Limit(), in.weight)))
			return
		}
		if !st.inFrames.TryTake(1) {
			c.rejectOrOverflow(st, in, reject,
				rejectErr(ScopeStream, DirInbound, st.id,
					quota.NewRejected(quota.ReasonMuxStreamInflightFrames, st.inFrames.Limit(), 1)))
			return
		}
		framesOK := st.inFrames.Limit() > 0
		if !st.inBytes.TryTake(in.weight) {
			st.inFrames.Give(1)
			c.rejectOrOverflow(st, in, reject,
				rejectErr(ScopeStream, DirInbound, st.id,
					quota.NewRejected(quota.ReasonMuxStreamInflightBytes, st.inBytes.Limit(), in.weight)))
			return
		}
		bytesOK := st.inBytes.Limit() > 0
		in.markHeld(st, false, framesOK, bytesOK)
	}

	// 2) 连接级（+全局）总在途配额，非阻塞；只在尚未持有时申请。
	in.mu.Lock()
	needConn := !in.heldConn
	in.mu.Unlock()
	if needConn {
		if qerr := c.acquireInboundConnTry(in); qerr != nil {
			c.rejectOrOverflow(st, in, reject, qerr)
			return
		}
	}

	// 3) 三级都拿到：投递该路 inCh（罕见满则防御性停放 pending，不阻塞分派器）。
	c.enqueueToStream(st, in)
}

// enqueueToStream 把已拿齐配额的帧送入该路并登记投递序号/END_STREAM 标记。
// 仅分派器调用。
func (c *Conn) enqueueToStream(st *Stream, in *inbound) {
	st.delivered++
	select {
	case st.inCh <- in:
	default:
		st.pending = append(st.pending, in)
	}
	if in.fr.EndStream() {
		st.endSeen = true
		st.mu.Lock()
		st.remoteSeen = true
		st.state = deriveStreamState(st.localSent, st.remoteSeen)
		st.mu.Unlock()
	}
}

// acquireInboundConnTry 非阻塞申请连接级+全局在途配额，失败返回 *QuotaError。
func (c *Conn) acquireInboundConnTry(in *inbound) *QuotaError {
	n := in.weight
	if c.connInflight.Limit() > 0 && n > c.connInflight.Limit() {
		return rejectErr(ScopeConn, DirInbound, 0,
			quota.NewRejected(quota.ReasonMuxConnInflightBytes, c.connInflight.Limit(), n))
	}
	if c.globalBytes != nil && c.globalBytes.Limit() > 0 && n > c.globalBytes.Limit() {
		return rejectErr(ScopeGlobal, DirInbound, 0,
			quota.NewRejected(quota.ReasonGlobalInflightBytes, c.globalBytes.Limit(), n))
	}
	heldGlobal := c.globalBytes != nil && c.globalBytes.Limit() > 0
	if heldGlobal && !c.globalBytes.TryTake(n) {
		return rejectErr(ScopeGlobal, DirInbound, 0,
			quota.NewRejected(quota.ReasonGlobalInflightBytes, c.globalBytes.Limit(), n))
	}
	if !c.connInflight.TryTake(n) {
		if heldGlobal {
			c.globalBytes.Give(n)
		}
		return rejectErr(ScopeConn, DirInbound, 0,
			quota.NewRejected(quota.ReasonMuxConnInflightBytes, c.connInflight.Limit(), n))
	}
	// 登记持有连接级配额；held 位与“是否已释放”完全分离。st 在路级申请
	// 成功时已通过 markHeld 登记。
	in.mu.Lock()
	in.heldConn = true
	in.mu.Unlock()
	return nil
}

// rejectOrOverflow 按策略处理一帧的配额不可得：
// reject → 路级 RST 该路 / 连接级终止连接（原因可区分维度与收/写）；
// block  → 停放到该路 overflow，等 kick 重试，绝不阻塞分派器。
func (c *Conn) rejectOrOverflow(st *Stream, in *inbound, reject bool, qerr *QuotaError) {
	if reject {
		in.releaseAll()
		if qerr.Scope == ScopeStream {
			c.logf("场景=路级在途配额拒绝 stream=%d 维度=%s 原因=%v", st.id, qerr.Scope, qerr)
			// 先向对端排队 RST（控制帧只占连接级待写配额），再本地重置。
			_ = c.enqueueReset(st.id, CloseReason{Code: 22, Text: qerr.Error(), Forced: false}, false)
			c.resetPeer(st.id, CloseReason{Code: 22, Text: qerr.Error(), Forced: false})
			return
		}
		c.logf("场景=连接级在途配额终止 维度=%s 原因=%v", qerr.Scope, qerr)
		c.forceClose(CloseReason{Code: 13, Text: qerr.Error(), Forced: true})
		return
	}
	st.overflow = append(st.overflow, in)
}

// pumpStream 在 worker 腾出在途槽位后：
//  1. 把防御性 pending 帧补进 inCh；
//  2. 严格按 FIFO 重试 overflow 队头的配额申请（成功才推进，绝不越过队头，
//     保证路内顺序）；队头仍拿不到配额则停下等下一次 kick；
//  3. 若对端已 END_STREAM 且彻底排空则推进收尾。
func (c *Conn) pumpStream(st *Stream) {
	// 1) 防御性 pending → inCh。
	for len(st.pending) > 0 {
		select {
		case st.inCh <- st.pending[0]:
			st.pending = st.pending[1:]
		default:
			return
		}
	}
	if st.isTerminalState() {
		return
	}
	// 2) FIFO 重试 overflow 队头：通过判定则弹出队头，继续下一帧；
	//    仍缺配额则帧留在队头，停下等下一次 kick（不越过队头→保序）。
	for len(st.overflow) > 0 {
		head := st.overflow[0]
		placed := c.tryPlace(st, head)
		if !placed {
			return
		}
		st.overflow = st.overflow[1:]
	}
	// 3) 对端 END_STREAM 且彻底排空 → 收尾。
	if st.peerHalfAndDrained() {
		c.maybeCloseStream(st)
	}
}

// tryPlace 尝试把 overflow 队头帧拿齐配额并投递；返回 false 表示仍缺配额，
// 帧保持在队头（不入 overflow 重复排队），等待下一次 kick。严格保序。
func (c *Conn) tryPlace(st *Stream, in *inbound) bool {
	if st.isTerminalState() {
		return false
	}
	// 路级帧数。
	in.mu.Lock()
	needFr := !in.heldFr
	needBy := !in.heldBy
	needConn := !in.heldConn
	in.mu.Unlock()
	if needFr {
		if !st.inFrames.TryTake(1) {
			return false
		}
		framesOK := st.inFrames.Limit() > 0
		if needBy && !st.inBytes.TryTake(in.weight) {
			st.inFrames.Give(1)
			return false
		}
		bytesOK := st.inBytes.Limit() > 0
		in.markHeld(st, false, framesOK, bytesOK)
	}
	if needConn {
		if qerr := c.acquireInboundConnTry(in); qerr != nil {
			return false
		}
	}
	c.enqueueToStream(st, in)
	return true
}

// tryPendingOpens 在有活跃路槽位释放后，按 FIFO 放行等待开路的帧。
func (c *Conn) tryPendingOpens() {
	limit := c.activeStreamsLimit()
	for len(c.pendingOpens) > 0 {
		if limit > 0 && c.active >= limit {
			return
		}
		in := c.pendingOpens[0]
		st := c.streams[in.fr.Stream]
		if st == nil {
			// 等待期间该路被重置：丢弃并归还。
			in.releaseAll()
			c.pendingOpens = c.pendingOpens[1:]
			delete(c.pendingOpenID, in.fr.Stream)
			continue
		}
		if st.opened || st.isTerminalState() {
			c.pendingOpens = c.pendingOpens[1:]
			delete(c.pendingOpenID, in.fr.Stream)
			if st.opened {
				c.deliverOrPark(st, in)
			} else {
				in.releaseAll()
			}
			continue
		}
		c.openStream(st)
		c.pendingOpens = c.pendingOpens[1:]
		delete(c.pendingOpenID, in.fr.Stream)
		c.deliverOrPark(st, in)
	}
}

// openStream 把一路计入活跃并启动 worker。
func (c *Conn) openStream(st *Stream) {
	st.opened = true
	st.peer = true
	c.active++
	c.activeN.Store(int64(c.active))
	c.wgWorkers.Add(1)
	go c.streamWorker(c.runCtx, st)
	c.logf("场景=逻辑路开启 stream=%d 活跃=%d", st.id, c.active)
}

// ---- 每路 worker ----

func (c *Conn) streamWorker(ctx context.Context, st *Stream) {
	defer c.wgWorkers.Done()
	for in := range st.inCh {
		func() {
			// 处理完成即归还在途配额（CAS 保证不重不漏），再 kick 分派器
			// 把该路停放帧按序补进 inCh。
			defer func() {
				in.releaseAll()
				st.processedAck.Add(1)
				c.kickN(st.id, 1)
			}()
			// 已 reset 的路：跳过剩余帧的应用回调（帧配额仍在 defer 归还）。
			if !st.isTerminalState() {
				c.handler.OnStreamFrame(st.ctx, st, in.fr)
			}
		}()
	}
	// inCh 关闭且排空：worker 退出，统一由分派器回收路槽（仅一次）。
	select {
	case c.evCh <- streamEvent{kind: evEnd, id: st.id}:
	case <-ctx.Done():
		// 分派器已随连接强制退出：无需也无法再上报。
	}
}

// ---- 写循环 ----

func (c *Conn) writeLoop(ctx context.Context) {
	defer c.wgIO.Done()
	var connHalfClosed bool
	for item := range c.outCh {
		n := int64(len(item.buf))
		if connHalfClosed && item.endStream && item.endStreamID == 0 && item.resetID == 0 {
			c.releaseOutbound(nil, n)
			continue
		}
		if c.opts.WriteTimeout > 0 {
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
		}
		_, werr := c.conn.Write(item.buf)
		// RST 只占连接级/全局待写配额，释放时不能带流句柄（避免误记路级）。
		if item.resetID > 0 {
			c.releaseOutboundConnOnly(n)
		} else {
			c.releaseOutbound(c.streamForRelease(item), n)
		}
		if werr != nil {
			if !c.isForced() && ctx.Err() == nil {
				c.forceClose(CloseReason{Code: 12, Text: "write failure: " + werr.Error(), Forced: true})
			}
			for rest := range c.outCh {
				rn := int64(len(rest.buf))
				if rest.resetID > 0 {
					c.releaseOutboundConnOnly(rn)
				} else {
					c.releaseOutbound(c.streamForRelease(rest), rn)
				}
			}
			return
		}
		c.signalActivity()
		switch {
		case item.resetID > 0:
			// RST 不改变连接写方向。
		case item.endStreamID > 0:
			c.markLocalStreamHalf(item.endStreamID)
		case item.endStream:
			if cw, ok := c.conn.(interface{ CloseWrite() error }); ok {
				_ = cw.CloseWrite()
			}
			connHalfClosed = true
		}
	}
}

func (c *Conn) streamForRelease(item *outItem) *Stream {
	switch {
	case item.endStreamID > 0:
		return c.lookupStream(item.endStreamID)
	case item.resetID > 0:
		return c.lookupStream(item.resetID)
	default:
		return nil
	}
}

// lookupStream 加锁快照查找路（写循环等非分派器 goroutine 使用）。
func (c *Conn) lookupStream(id uint32) *Stream {
	c.tableMu.RLock()
	defer c.tableMu.RUnlock()
	return c.streams[id]
}

// ---- 空闲看门狗 ----

func (c *Conn) idleWatchdog(ctx context.Context) {
	defer c.wgIO.Done()
	t := time.NewTimer(c.opts.IdleTimeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-c.activity:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(c.opts.IdleTimeout)
		case <-t.C:
			c.forceClose(ReasonIdleTimeout)
			return
		}
	}
}

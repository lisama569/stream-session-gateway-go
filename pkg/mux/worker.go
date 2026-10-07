package mux

import (
	"sync/atomic"
)

// job.releaseOnce 保证一个任务持有的各级入站配额在任何路径下只归还一次：
// 正常处理完由 worker 归还；流/连接被强制终止时由强制路径代为归还，
// 之后 worker 若结束任务再归还则是空操作（不多还）。
func (j *job) releaseOnce(m *Mux, s *Stream) {
	j.once.Do(func() {
		weight := j.weight
		s.inBytes.Give(weight)
		s.inFrames.Give(1)
		m.giveConnInbound(1, weight)
		if m.deps.GlobalBytes != nil {
			m.deps.GlobalBytes.Give(weight)
		}
	})
}

// giveConnInbound 归还连接级入站配额。
func (m *Mux) giveConnInbound(frames, bytes int64) {
	if m.deps.ConnInflightFrames != nil {
		m.deps.ConnInflightFrames.Give(frames)
	}
	if m.deps.ConnInflightBytes != nil {
		m.deps.ConnInflightBytes.Give(bytes)
	}
}

// runWorker 是每一路唯一的串行处理器：路内帧严格按接收顺序执行。
// jobCh 由唯一发送方（读循环）在正常输入结束时关闭；异常终止不关闭，
// worker 会在当前任务结束后看到 stopCh 并退出，保证已取走的任务恰好执行一次。
func (s *Stream) runWorker() {
	defer s.workerWG.Done()
	for {
		select {
		case <-s.stopCh:
			// 异常终止：停止处理。已在队列中的任务配额由强制路径统一归还。
			s.m.logf("场景=逻辑流worker终止 stream=%d", s.id)
			return
		case j, ok := <-s.jobCh:
			if !ok {
				// jobCh 已关闭：排空关闭前已入队的残余任务（不丢失已接收帧）。
				// 注意：关闭且为空时首次接收即返回 (nil,false)，j 不可用。
				for j := range s.jobCh {
					s.invoke(j)
				}
				s.finishNormal()
				return
			}
			s.invoke(j)
			// 每个任务后检查：双向半闭时及时收尾。
			if !s.control {
				s.tryFinishAfterJob()
			}
		}
	}
}

// invoke 执行单个任务并在结束时归还入站配额（恰好一次）。
// 流上下文随终止而取消，便于有礼貌的处理器解除阻塞；无礼的处理器
// 忽略 ctx 只会影响它自己这一路（连接关闭路径不等待它）。
func (s *Stream) invoke(j *job) {
	select {
	case <-s.stopCh:
		// 终止发生在取任务与执行之间：不执行，归还全部配额。
		j.releaseOnce(s.m, s)
		return
	default:
	}
	s.currentJob.Store(j)
	defer s.currentJob.Store(nil)
	func() {
		defer j.releaseOnce(s.m, s)
		s.m.handler.OnStreamFrame(s.ctx, s, j.fr)
	}()
}

// tryFinishAfterJob 在双向半闭（BothHalf）且该路队列已处理空时进入正常完成。
// 仅对端半闭（RemoteHalf）不在此收尾：本端可能还要继续 Send/CloseWrite，
// 必须等本端半闭成 BothHalf，或连接级关输入（closeIntake→jobCh 关闭）后
// 由 worker 的 channel-close 分支统一收尾。
func (s *Stream) tryFinishAfterJob() {
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	if st == StreamBothHalf {
		s.finishNormal()
	}
}

// tryFinishAfterWorker 在 CloseWrite 把状态推到 BothHalf、但 worker 可能
// 已经因输入关闭而退出时兜底收尾。
func (s *Stream) tryFinishAfterWorker() {
	if s.control {
		return
	}
	s.mu.Lock()
	st := s.state
	s.mu.Unlock()
	if st != StreamBothHalf {
		return
	}
	// worker 若仍在跑，会在 tryFinishAfterJob 收尾；这里仅当输入已关且
	// worker 即将/已经退出时补偿。等一拍让 worker 处理完残余任务。
	if s.m.isIntakeClosed() {
		go func() {
			s.workerWG.Wait()
			s.finishNormal()
		}()
	}
}

// finishNormal 把流推进到正常吸收态（幂等，保留首次终态）。
func (s *Stream) finishNormal() {
	s.mu.Lock()
	if s.isTerminalLocked() {
		s.mu.Unlock()
		return
	}
	s.state = StreamClosed
	s.reason = ""
	s.cancel()
	s.mu.Unlock()

	s.doneOnce.Do(func() { close(s.done) })
	if !s.control {
		s.m.slotGateGive()
		atomic.AddInt64(&s.m.stats.ClosedNormally, 1)
		s.m.retireStream(s)
	}
	s.m.emit(s.id, EventClosed, "state="+StreamClosed.String())
	s.m.logf("场景=逻辑流正常收尾 stream=%d", s.id)
}

// retireStream 从存活表摘除。存活表规模因此始终受“活跃路数上界”约束，
// 已收尾流不再占用任何记账与内存（句柄仅在处理器/观察者持有期间存活）。
func (m *Mux) retireStream(s *Stream) {
	m.mu.Lock()
	delete(m.streams, s.id)
	m.mu.Unlock()
}

func (m *Mux) isIntakeClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.intake
}

// closeIntake 标记输入关闭并关闭所有存活流的 jobCh（每个 jobCh 唯一发送方
// 就是读循环，此时它不会再投递，关闭安全）。
func (m *Mux) closeIntake() {
	m.mu.Lock()
	if m.intake {
		m.mu.Unlock()
		return
	}
	m.intake = true
	live := make([]*Stream, 0, len(m.streams))
	for _, s := range m.streams {
		live = append(live, s)
	}
	m.mu.Unlock()
	for _, s := range live {
		s.closeJobOnce.Do(func() { close(s.jobCh) })
	}
}

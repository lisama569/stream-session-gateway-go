# 本地双向流会话网关（Go）

在本地 TCP 网络之上并发接入多个客户端，提供**多版本二进制帧协议**的双向收发、
**两级配额背压**与**可解释的关闭生命周期**。目标是在半包、粘包、坏帧、配额
超限、停机竞态等异常链路下，行为一致、可复现、可解释：不静默丢帧、不串包、
不透出底层错误、内存不随连接数无界增长。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `pkg/frame` | 多版本帧编解码；字节流增量解码（半包/粘包）、坏帧重同步 |
| `pkg/quota` | 加权配额器（FIFO 公平背压/立即拒绝）与连接数闸门 |
| `pkg/session` | 单连接会话：读写循环、配额联动、半关闭/宽限/强制终止状态机 |
| `pkg/mux` | **一条连接上的多路逻辑流复用**：按 StreamID 分派、路内保序、跨路互不阻塞、路级/连接级配额、单路半关闭/重置 |
| `pkg/gateway` | 本地监听器：并发接入、全局配额、停机生命周期（支持单路/多路两种后端） |
| `cmd/demo` | 可运行的 echo 冒烟程序（`-mux` 开启多路） |

## 1. 帧协议与兼容范围

### 1.1 统一帧布局

```
偏移 0:     SOF        1 字节，固定 0xAA（字节流重同步锚点）
偏移 1:     Version    1 字节（1 = V1，2 = V2）
偏移 2:     Flags      1 字节（bit0 = END_STREAM 半关闭；bit1 = RST_STREAM 单流重置，仅多路语义）
偏移 3-4:   Length     2 字节大端，负载字节数（最大 32768 = 32KiB）
偏移 5-8:   StreamID   4 字节大端
偏移 9..:   Payload    Length 字节
帧尾:       Checksum   V1 = 1 字节负载逐字节 XOR
                       V2 = 2 字节 CRC-16/CCITT-FALSE（poly 0x1021，init 0xFFFF）
```

### 1.2 兼容策略

- **同时兼容 V1 与 V2**：同一条连接上可交替出现两种版本，解码器按帧头版本
  选择对应校验算法，无需握手升级。
- 可选 `Options.NegotiatedVersion` 把会话钉死到单一版本；收到其他版本帧
  **不静默丢弃**，而是以协议错误（code=11）强制终止并保留原因。
- 出站帧由发送方选择版本；`CloseWrite` 的收尾 END_STREAM 固定使用 V2。

### 1.3 字节流异常的区分与恢复

解码器是有状态的增量解析器，内部只保留未消费残余，遇到异常按下表区分，
且**坏帧后自动重同步到下一个 SOF，调用方可继续读取后续好帧**（不串包）：

| 现象 | 哨兵错误 | 处理 |
| --- | --- | --- |
| 半包（帧未到齐） | 无（内部缓冲等待） | 保留 SOF 与残余，继续读 |
| 粘包（一帧多帧） | 无 | 依次切出多帧 |
| 版本号 ∉ {1,2} | `frame.ErrUnknownVersion` | 跳过该候选 SOF，重同步 |
| Length > 32KiB | `frame.ErrLengthExceeded` | 长度字段不可信，重同步 |
| 校验不符 | `frame.ErrChecksum` | 重同步 |
| 流尾部帧被截断 | `io.ErrUnexpectedEOF` | 终止连接 |
| 对端正常结束 | `io.EOF` | 走半关闭/收尾 |
| 底层 I/O 故障 | 原始错误（仅此情况透出） | 以 code=12 强制终止 |

- 所有帧级错误都可用 `errors.Is` 精确判定；连续坏帧达到 8 次（`maxProtocolErrors`）
  才终止连接，避免一次性故障拖垮会话，也避免异常链路无限空转。
- 解码器在“始终找不到 SOF”时设有残余上限（1 MiB），防止垃圾数据导致内存无界增长。

> 说明：底层 I/O 错误是唯一被透出的错误类别；协议级错误一律被翻译成上述
> 哨兵/关闭原因，不把 `net` 包错误直接抛给应用帧处理逻辑。

## 2. 配额与背压语义

配额分两级，申请顺序固定为 **全局 → 会话**，杜绝跨维度死锁。

| 维度 | 配置 | 触发后的可区分原因（`quota.Reason`） |
| --- | --- | --- |
| 全局并发连接数 | `Config.MaxConnections` | 拒入，计数 `Stats.RejectedConnQuota` |
| 全局在途/待写字节 | `Config.GlobalBytes` | `global_inflight_bytes_exceeded` |
| 会话在途帧数 | `Options.MaxInflightFrames` | `session_inflight_frames_exceeded` |
| 会话在途字节 | `Options.MaxInflightBytes` | `session_inflight_bytes_exceeded` |
| 会话待写字节 | `Options.MaxOutboundBytes` | `session_outbound_bytes_exceeded` |

### 2.1 两种策略

- **拒绝（PolicyReject）**：立即返回 `*quota.RejectedError`，可用
  `errors.Is(err, quota.ErrRejected)` 统一判定、`errors.As` 取出
  `Reason/Limit/Asked`。入站被拒按 code=13 终止会话（不能把已读到的帧
  悄悄塞回管道）；出站被拒把错误交给调用方，连接保持。
- **背压（PolicyBlock，默认）**：调用方阻塞在 FIFO 等待队列上，直到拿到
  配额或 `ctx` 结束。队列严格 FIFO：队头权重不满足时**不会越过它**授予
  后来者，防止饥饿；`ctx` 取消会从队列摘除并保证配额不泄漏。

### 2.2 “不丢失、不重复、内存有界”如何保证

- 帧在“已解码 → 占配额 → 交给处理器”这一区间始终有配额记账；处理器结束
  时用 `defer` 归还（帧权重 = 负载字节数 + 1 帧）。
- 接收侧背压直接作用于读循环：在途配额满时**停止从连接读字节**，把背压
  沿 TCP 反压到对端，因此已接收数据不会堆积，内存不随连接/流量无界增长。
- 默认**串行处理**：读循环只解码，解码后经无缓冲 `jobCh` 交给唯一 worker
  按接收顺序处理，天然不串包、不乱序。需要应用级并发时显式设置
  `Options.ConcurrentHandlers=true`，此时仍受在途帧数/字节配额约束。
- 出站 `Send` 先编码计费再入队；写循环是唯一消费者，串行写出保证帧不交错。
  通道关闭与 `Send` 之间用互斥锁 + `outClosed` 标志保护，杜绝
  “send on closed channel”竞态与配额重复归还。

## 2.5 一条连接跑多路逻辑流（`pkg/mux`）

帧头的 `StreamID`（偏移 5-8）在多路模式下成为**逻辑流标记**。同一条 TCP
连接可同时承载任意数量互不相干的流，由 `pkg/mux` 在单路上方做复用：

```go
g := gateway.NewMux(mux.HandlerFunc(func(ctx context.Context, st *mux.Stream, f *frame.Frame) {
    // f.Stream 即该帧所属的流；st 是这一路的句柄
    _ = st.Send(ctx, f)      // 在本路上回写
    // st.CloseWrite()       // 只半关闭这一路
    // st.Reset(reason)      // 只重置这一路
}), cfg)
```

### 2.5.1 分派模型：路内保序、跨路互不阻塞

内部拆成四类 goroutine，职责彼此解耦：

- **读循环**：只负责从字节流增量解码（复用 `frame.Decoder`，天然处理半包/
  粘包/坏帧重同步），解出一帧即交给分派器，**不在任何一路上阻塞**；
- **分派器（唯一路由 goroutine）**：按 `StreamID` 找到/建立对应流；所有配额
  判定都是**非阻塞 TryTake**——某一路满了就把帧停放到**该流自己的**
  overflow 队列并立刻继续服务其它流，绝不因一路卡住而停读、拖死整条连接；
- **每流一个 worker**：该流的帧严格经一条带缓冲 `inCh` 串行投递给
  `OnStreamFrame`，因此**路内严格保序、不串包**；不同流的 worker 并发执行；
- **单写循环**：所有流的出站帧汇入唯一 `outCh` 串行写出，帧在字节层面不交错。

因此某一路处理慢、卡住或永不结束，只会占满它自己的在途/停放额度，
**其它流的收发照常进行**（见 `TestCrossStreamNoBlocking`）。

### 2.5.2 多路配额与背压语义

多路模式下配额分为**路级 / 连接级 / 全局**三级，出站方向同样三级：

| 维度 | 配置（`mux.Options`） | 触发原因 |
| --- | --- | --- |
| 活跃流数 | `MaxActiveStreams`（默认 256） | `mux_active_streams_exceeded` |
| 单流在途帧数 | `MaxStreamInflightFrames`（默认 64） | `mux_stream_inflight_frames_exceeded` |
| 单流在途字节 | `MaxStreamInflightBytes`（默认 256KiB） | `mux_stream_inflight_bytes_exceeded` |
| 单流待写字节 | `MaxStreamOutboundBytes`（默认 256KiB） | `mux_stream_outbound_bytes_exceeded` |
| 连接总在途字节 | `MaxConnInflightBytes`（默认 4MiB） | `mux_conn_inflight_bytes_exceeded` |
| 连接总待写字节 | `MaxConnOutboundBytes`（默认 2MiB） | `mux_conn_outbound_bytes_exceeded` |
| 全局字节 | `gateway.Config.GlobalBytes` | `global_inflight_bytes_exceeded` |

拒绝/背压策略由 `InflightPolicy` 控制，错误统一为 `*mux.QuotaError`，
可用 `mux.IsQuotaError` 取出并**同时区分两个正交维度**：

- `qe.Scope`：`ScopeStream`（路级）/ `ScopeConn`（连接级）/ `ScopeGlobal`（全局）；
- `qe.Direction`：`DirInbound`（收不下）/ `DirOutbound`（写不出）；
- 另有 `qe.Reason / Limit / Asked / Stream` 明细，`errors.Is(err, quota.ErrRejected)`
  亦成立。

策略语义：

- **PolicyReject（拒绝）**：单流在途超限 → 该流被立即重置并向对端发
  `RST_STREAM`（帧标志位 `FlagReset=0x02`，负载为可解释原因），**其它流与
  整条连接不受影响**；连接级/全局在途收不下 → 无法把已解码帧塞回管道，
  按 code=13 终止整条连接（原因可区分是连接级还是全局）。`Send` 待写被拒
  则把错误交给调用方，连接保持。
- **PolicyBlock（背压，默认）**：不丢帧。某流暂时拿不到配额时，帧停放在该流
  的 overflow FIFO（不占连接总在途额度），等该流前面的帧处理完、配额释放后，
  分派器严格按 FIFO 重试（不越过队头→保序）；连接总在途满时读循环停止读字节，
  沿 TCP 反压到对端。内存始终被“活跃流数 × 单流上限 + 连接总量”封顶。

### 2.5.3 不丢帧、不重复、内存有界

- 每帧的每个配额维度用独立的 held/released 标记，CAS 保证**每维恰好归还一次**；
  worker `defer` 归还在途配额，写循环归还待写配额。流结束（正常/重置）或
  连接强制终止时，overflow/pending/rawCh 中残余帧统一释放，配额归零
  （`TestQuotaReleasedAfterStreamEnd` 用 200 流验证 active=0、两级 used=0）。
- 已被 worker 接收的帧在流重置时不重复回调：重置只丢弃后续帧，已在处理的
  帧完成后正常归还配额。
- 活跃流槽位在 worker 退出后回收；`MaxActiveStreams` 背压时等待开路的帧按
  FIFO 放行，槽位归还后被停放的流继续（`TestActiveStreamLimitBackpressure`）。

### 2.5.4 多路关闭与异常

- **单流半关闭**：`st.CloseWrite()` 只对该流排队一个 `END_STREAM`（不做 socket
  FIN），该流进入本端半闭；同连接其它流完全不受影响。收到对端某流
  `END_STREAM` 只标记该流对端半闭。双方都半闭且该流帧全部处理完后，该流进入
  closed 吸收态并回收槽位（`TestStreamHalfCloseIsolation`）。
- **单流重置**：`st.Reset()` 或收到对端 `RST_STREAM` 只终止这一路；RST 是
  连接级控制帧，**只占连接级/全局待写额度**，即使被重置流的待写额度已满也能
  把 RST 发出去。并发/重复调用安全（`TestConcurrentStreamResetIdempotent`）。
- **整条连接 / 网关停机**：`Conn.GracefulClose(grace)` 后宽限期内让在途流把
  结果处理完；到点仍有流卡住则以 `ReasonGraceExpired`(code=3) 强制终止该连接
  及全部流；经网关停机时由网关以 `ReasonGatewayDown`(code=4) 兜底。
  某一路卡在 handler 里（只要它尊重 `st.Context()`）不会挂死停机
  （`TestGraceExpiryOneStreamStuck`、`gateway.TestMuxGatewayShutdownOneStuck`）。
- `StreamID=0` 保留为**连接级控制帧**：`END_STREAM` 表示整条连接对端半闭；
  StreamID=0 上出现数据帧按协议错误处理。对端彻底关闭连接（EOF）等价于所有
  流隐式对端半闭，分派器把残余流推进收尾。
- **坏字节穿插**：读循环复用帧解码器，多路帧之间混入未知版本/坏校验/半包/
  粘包时自动重同步到下一个 SOF，恢复后各流归属与路内顺序仍正确、好帧不丢
  （`TestGarbageInterleavedAcrossStreams`）。

### 2.5.5 `mux.Options` 默认值与兼容范围

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `MaxActiveStreams` | 256 | 同时活跃流上限，<=0 不限 |
| `MaxConnInflightBytes` | 4 MiB | 连接总在途负载字节 |
| `MaxConnOutboundBytes` | 2 MiB | 连接总待写（编码后）字节 |
| `MaxStreamInflightFrames` | 64 | 单流在途帧数 |
| `MaxStreamInflightBytes` | 256 KiB | 单流在途负载字节 |
| `MaxStreamOutboundBytes` | 256 KiB | 单流待写字节 |
| `StreamQueueFrames` | 64 | 单流 `inCh` 容量（跨路隔离的关键） |
| `InflightPolicy` | Block | Reject=立即拒绝/RST，Block=按流背压 |
| `NegotiatedVersion` | 0（V1/V2 都接受） | 钉死单版本 |
| `IdleTimeout/GracePeriod/WriteTimeout` | 0 / 5s / 10s | 同单路语义 |

兼容范围：流复用建立在既有帧头之上，**线协议不变**，仅新增一个标志位
`FlagReset(0x02)` 表示 RST_STREAM；旧版只识别 `END_STREAM(0x01)` 的对端会把
RST 当作普通帧（其负载为原因文本）。多路与单路（`pkg/session`）在网关层
通过 `gateway.NewMux` / `gateway.New` 二选一，互不影响。

## 3. 关闭生命周期语义

```
Active ──CloseWrite──▶ LocalHalf ──收END_STREAM──▶ (GracefulClose) Closing
Active ──收END_STREAM─▶ RemoteHalf ──CloseWrite──▶ (GracefulClose) Closing
Active/Half ─────────GracefulClose──────────────▶ Closing
任意状态 ───────────ForceClose──────────────────▶ ForceTerminated
Closing ──排空完成──▶ Closed；  Closing ──宽限耗尽──▶ ForceTerminated
```

`Closed` 与 `ForceTerminated` 是吸收态，不回退；重复关闭幂等，**保留首次
关闭原因**。

| 操作 | 语义 |
| --- | --- |
| `CloseWrite()` | 半关闭发送侧：排队写出 END_STREAM，成功后对底层连接做 TCP `CloseWrite`（FIN）。幂等；其后 `Send` 返回 `ErrHalfClosed` |
| 收到对端 END_STREAM | 迁移到 RemoteHalf，读侧等待本端动作；双方都半闭后自动进入排空 |
| `GracefulClose(grace)` | 进入 Closing：宽限期内继续读完并处理“对端已发出、尚在途中”的帧；已接收数据的处理器可继续 `Send` 结果。读截止到达后停止接收，等待在途处理器与待写帧排空 |
| 宽限耗尽 | 以 `ReasonGraceExpired`（code=3，Forced）强制终止，关闭 socket 解除一切阻塞 |
| `ForceClose(reason)` | 立即关 socket、取消内部 ctx；不等待处理器（处理器应响应 ctx）。原因被保留，`Reason()`/最终 error 可解释 |
| 空闲超时 | 任意方向静默达到 `IdleTimeout` 即 `ReasonIdleTimeout`（code=2）强制终止 |
| 对端 FIN/EOF | 正常收尾，`ReasonPeerEndStream`（code=1） |

并发安全：`ForceClose`/`GracefulClose`/`CloseWrite`/`Send` 可从多个 goroutine
并发调用；`Run` 重复调用安全（后续调用等待同一结果）。测试用 16 路并发
强制关闭验证不 panic、不回退、首次原因不被覆盖。

### 网关停机

1. `Shutdown` 先关监听器并置 `closed`：停机期间竞态 accept 到的连接会被
   立即拒入（计数 `RejectedWhileShutdown`），OS 层的新连接直接被拒；
2. 对全部在途会话发起 `GracefulClose(grace)`；
3. 宽限内全部结束则干净返回；到期仍存活的会话统一以
   `ReasonGatewayDown`（code=4）强制终止；
4. `Shutdown` 重复/并发调用幂等。

## 4. 配置项

### 会话 `session.Options`

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `NegotiatedVersion` | 0（V1/V2 都接受） | 钉死单版本 |
| `MaxInflightFrames` | 1024 | 会话在途帧数上限，<=0 不限 |
| `MaxInflightBytes` | 4 MiB | 会话在途字节上限，<=0 不限 |
| `MaxOutboundBytes` | 2 MiB | 待写队列字节上限，<=0 不限 |
| `InflightPolicy` | Block | Reject=立即拒绝，Block=背压 |
| `ConcurrentHandlers` | false | true=每帧独立 goroutine（默认串行保序） |
| `IdleTimeout` | 0（关闭） | 双向空闲超时 |
| `GracePeriod` | 5s | 宽限排空时长 |
| `WriteTimeout` | 10s | 单次写超时，<=0 不设 |

### 网关 `gateway.Config`

`Listen`（默认 127.0.0.1:0 随机端口）、`MaxConnections`、`GlobalBytes`、
`Session`（会话选项）、`ShutdownGrace`（默认 5s）、`OnSessionEnd`
（会话终结回调，可观测状态与原因）。计数器见 `gateway.Stats()`：
`Accepted / RejectedConnQuota / RejectedWhileShutdown`。

## 5. 本地验证

### 5.1 运行全部测试（建议带 race）

```bash
go build ./...
go vet ./...
go test ./... -race -count=1
```

### 5.2 测试覆盖矩阵

| 测试 | 场景 | 判定依据 |
| --- | --- | --- |
| `frame.TestRoundTripV1V2` | V1/V2 混合、空帧、大负载 | 逐字段比对往返 |
| `frame.TestSplitAndSticky` | 1/1/2/3/5 字节极端分片 + 粘包 | 3 帧全部还原 |
| `frame.TestCorruptRecovery` | 未知版本/校验坏/长度越界混合在好帧之间 | 三类哨兵错误各被识别，3 个好帧不丢不乱 |
| `frame.TestTruncatedTail` | 流尾截断帧 | `io.ErrUnexpectedEOF` |
| `quota.TestBlockingFIFO` | FIFO 公平背压 | 只放够队头的量时仅队头被授予 |
| `quota.TestTakeCancelReturnsQuota` | 等待中取消 | 取消后配额无泄漏 |
| `session.TestEchoBidirectional` | 双向多帧回显 | 顺序与内容一致 |
| `session.TestHalfCloseBothSides` | 双向半关闭 | 对端观察到 END_STREAM 且正常关闭 |
| `session.TestIdleTimeout` | 空闲超时 / 有流量不误杀 | code=2；持续流量保持 active |
| `session.TestForceCloseIdempotent` | 16 路并发重复强制关闭 | 无 panic、首次原因保留 |
| `session.TestGracefulDrain` | 宽限排放在途帧 | 2 帧处理完才关闭，不丢失 |
| `session.TestGraceExpiryForceTerminates` | 宽限耗尽 | code=3 强制终止 |
| `session.TestInboundBackpressureBounded` | 在途配额背压 | 峰值在途=4，解除后 20 帧恰好处理一次 |
| `gateway.TestConcurrentAccept` | 40 客户端并发 | 全部 echo 成功，Accepted=40 |
| `gateway.TestConnectionQuotaReject` | 连接数超限 | 10 个溢出连接全部无 echo 且计数正确 |
| `gateway.TestGlobalBytesQuotaReject` | 全局字节预算超限 | 会话以配额原因 code=13 终止 |
| `gateway.TestShutdownRace` | 8 路并发 Shutdown + 停机后拨号 | 无 panic、active=0、新连接被拒 |
| `mux.TestMultiStreamConcurrencyOrdering` | 8 流 ×25 帧交错并发 | 各路 seq 严格递增、跨流不串包 |
| `mux.TestCrossStreamNoBlocking` | 一路永久卡住 | 卡住期间其它 3 路 5 轮往返照常 |
| `mux.TestStreamInflightQuotaReject` | 路级在途配额拒绝 | 收到 RST 且原因为路级收不下；连接存活、其它路正常 |
| `mux.TestStreamOutboundQuotaReject` | 路级待写配额拒绝 | QuotaError 标记 scope=stream/dir=outbound |
| `mux.TestStreamHalfCloseIsolation` | 单流半关闭 | END_STREAM 往返、连接存活、其它流继续 echo |
| `mux.TestActiveStreamLimitBackpressure` | 活跃流上限背压 | 一路结束归还槽位后停放流 FIFO 放行不丢帧 |
| `mux.TestQuotaReleasedAfterStreamEnd` | 200 流依次结束 | active=0、连接两级在途/待写 used=0，无泄漏 |
| `mux.TestGraceExpiryOneStreamStuck` | 停机宽限到期一路卡住 | code=3 强制终止，其它流结果已处理完 |
| `mux.TestGarbageInterleavedAcrossStreams` | 坏版本/坏校验/垃圾穿插多路 | 4 个好帧跨 3 路全部还原、各路内有序 |
| `mux.TestConcurrentStreamResetIdempotent` | 16 路并发 Reset + ForceClose | 无 panic、状态不回退 |
| `gateway.TestMuxGatewayShutdownOneStuck` | 多路连接一路卡住时网关停机 | Shutdown 按时返回、active=0、原因 code=3/4 |

单测日志统一打印“场景 / 各路状态 / 判定依据”，多路用例还会打印活跃流数、
配额维度与原因，可用 `-v` 查看完整轨迹。所有用例均在 `-race -count=1`
（竞态检测 + 每次重新执行、不吃缓存）下验证。

### 5.3 手工冒烟

单路模式（默认）：

```bash
go run ./cmd/demo -addr 127.0.0.1:9000 -idle 30s -grace 3s
```

多路模式：

```bash
go run ./cmd/demo -mux -addr 127.0.0.1:9000 -grace 3s
```

服务端打印监听地址后，用 Go 客户端 `frame.Encode` 发送带不同 `StreamID` 的
V1/V2 帧，即可在同一条连接上并发收到各路原样回显；给某一流发带
`END_STREAM` 的帧只半关闭该流；发满某路在途配额可观察到该路收到
`RST_STREAM` 而其它路不受影响；Ctrl+C 观察宽限排空停机（卡住的流到点被
强制终止）与最终 `stats` 输出。

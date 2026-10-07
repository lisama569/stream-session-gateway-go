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
| `pkg/mux` | 一条连接上的多路逻辑流：按 StreamID 分派、流级/连接级配额、独立半关闭与宽限 |
| `pkg/gateway` | 本地监听器：并发接入、全局配额、停机生命周期（可选多路模式） |
| `cmd/demo` | 可运行的 echo 冒烟程序（`-mux` 切到多路模式） |

## 1. 帧协议与兼容范围

### 1.1 统一帧布局

```
偏移 0:     SOF        1 字节，固定 0xAA（字节流重同步锚点）
偏移 1:     Version    1 字节（1 = V1，2 = V2）
偏移 2:     Flags      1 字节（bit0 = END_STREAM 半关闭）
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

## 3. 一条连接多路逻辑流（multiplexing）

帧头里的 4 字节 `StreamID` 就是逻辑流标记。启用多路模式后，**同一条 TCP
连接可同时承载多路互不相干的请求/订阅**，复用器按 StreamID 把帧分派到各自
的处理路子。

### 3.1 使用方式

会话级：构造会话后、`Run` 之前调用 `SetStreamHandler`（即启用多路模式）：

```go
sess := session.New(id, conn, nil, opts) // 多路模式下单路 Handler 传 nil
sess.SetStreamHandler(session.StreamHandlerFunc(func(ctx context.Context, st *session.Stream, f *frame.Frame) {
    _ = st.Send(ctx, f)                 // 在该路上回写，自动写回流 ID
    // _ = st.CloseWrite(ctx)           // 只半关闭这一路，不连累其它路
}))
go sess.Run(ctx)
```

网关级：在 `gateway.Config` 设置 `StreamHandler`，所有接入连接即跑多路模式
（与单路 `Handler` 二选一，同时设置以 `StreamHandler` 为准）。

- `st.ID()` 为该路 StreamID；`st.Send` / `st.CloseWrite` / `st.Abort` 作用于
  单路；`st.Done()` 在该路进入吸收态后关闭。
- **stream 0 是连接级控制流**：其 END_STREAM 表示整条连接写侧半关闭（TCP
  FIN 语义），与单路模式的 `CloseWrite` 等价；应用数据请使用非 0 StreamID。

### 3.2 顺序与隔离语义

- **路内严格保序、不串包**：每一路只有一个 worker，帧严格按接收顺序执行；
  同一路的多次 `Send/CloseWrite` 也按调用顺序编码，不会交错。
- **跨路并发、互不阻塞**：不同路由不同 goroutine 处理。某一路处理慢、卡住
  甚至永不结束，只会顶满它自己的在途队列并对“读循环”施加背压；其它路的
  worker 与共享写循环照常收发，不会被一路拖死。
- 写循环仍是整条连接唯一的串行消费者，保证不同路的字节在物理连接上不交错；
  帧自带 StreamID，接收端据此重新归属，跨路完成先后不影响各路内部顺序。

### 3.3 流级与连接级配额、背压

申请顺序固定为 **全局 → 连接 → 单路**，任何一级失败都回滚已申请量，
从根上避免跨维度死锁与配额泄漏。

| 维度 | 配置 | 拒收原因 `quota.Reason` |
| --- | --- | --- |
| 连接上活跃路数 | `mux.Config.MaxStreams` | `conn_active_streams_exceeded` |
| 连接总在途帧/字节 | `Options.MaxInflightFrames/Bytes` | `session_inflight_*_exceeded` |
| 单路在途帧/字节 | `mux.Config.MaxStreamInflightFrames/Bytes` | `stream_inflight_*_exceeded` |
| 单路待写字节 | `mux.Config.MaxStreamOutboundBytes` | `stream_outbound_bytes_exceeded` |
| 网关全局字节 | `gateway.Config.GlobalBytes` | `global_inflight_bytes_exceeded` |

拒收/背压与可区分原因：

- 配额错误统一为 `*mux.RejectError`，可用 `errors.As` 取出
  **Scope（stream/connection/global）× Direction（recv 收不下 / write 写不出）
  × Reason × StreamID × Limit/Asked**，并用 `errors.Is(err, mux.ErrQuotaRejected)`
  统一判定。
- **背压（PolicyBlock，默认）**：收方向配额满时读循环停止从连接读字节，把
  背压经 TCP 反推给对端；只阻塞“往该路投递”，不阻塞其它路。`Send` 待写配额
  满时阻塞该调用方（FIFO 公平）。
- **拒收（PolicyReject）**：
  - 连接级/全局级“收不下”：无法静默塞回字节流，按 code=13 终止整条连接，
    原因里带 scope/方向；
  - 单路“收不下”：**只终止该路并丢弃本帧**（`Stats.RejectedStreams` 计数、
    `reject_inbound` 事件），读循环继续服务其它路；
  - “写不出”：错误返回给该路 `Send` 的调用方，连接与其它路保持。

### 3.4 内存有界、不丢不重、配额归还

- 帧在“已解码 → 占配额 → 入该路队列 → worker 处理完”全程记账；每路配额用
  `sync.Once` 收口，**任何结束/异常路径恰好归还一次**，不多还、不少还。
- 流正常收尾或异常终止后立即从存活表摘除并归还活跃路数槽位与各级配额，
  **存活表规模始终 ≤ `MaxStreams`**，内存不随累计路数/连接数无限增长。
- 已接收的好帧：背压时只是停止读，不会丢；正常排空时该路 worker 把队列里
  的帧恰好处理一次。连接强制终止时，滞留队列与“正在执行但处理器忽略 ctx”
  的当前帧，其连接级记账都被立即回收（处理器事后返回时的归还成为空操作）。

### 3.5 多路关闭语义

单路独立状态机（只进不退）：

```
Active ──本端CloseWrite──▶ LocalHalf ──对端END_STREAM──▶ BothHalf ─在途排空─▶ Closed
Active ──对端END_STREAM──▶ RemoteHalf ─本端CloseWrite──▶ BothHalf ─在途排空─▶ Closed
任意状态 ──st.Abort / 连接强制终止 / 宽限到期──▶ Aborted
```

- **单路半关闭**：`st.CloseWrite()` 只给该流排队一个 END_STREAM；一路双向
  半闭正常收尾，不影响同连接其它活跃路，也不会半关闭底层连接。
- **连接/网关停机**：`GracefulClose`/`Shutdown` 进入宽限后排空——宽限期内
  在途各路继续处理并可 `Send` 结果；**宽限到点仍卡着的路被强制终止**，
  `WaitDrain` 返回这些卡住流的 ID，会话以 `ReasonGraceExpired`（code=3，
  原因文本含卡住流）终止，停机绝不被一路挂死。
- **重复/并发关闭稳定**：Closed/Aborted 是吸收态，重复 `Abort/CloseWrite/
  ForceClose` 幂等、不 panic、状态不回退，首次原因保留。
- **坏字节穿插多路**：坏帧（未知版本/长度越界/校验不符）由帧解码器重同步
  到下一个 SOF，多路归属依赖帧内 StreamID 而非字节位置，因此恢复后各路的
  归属与顺序仍正确，好帧一个不丢（连续坏帧达 8 次才终止连接）。

`mux.Stats()` 提供 `Opened / ClosedNormally / Aborted / RejectedStreams /
ConnRejectedIn` 计数；`sess.ActiveStreams()`、`sess.MuxStats()` 可观测。

## 4. 关闭生命周期语义

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

## 5. 配置项

### 会话 `session.Options`

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `NegotiatedVersion` | 0（V1/V2 都接受） | 钉死单版本 |
| `MaxInflightFrames` | 1024 | 会话在途帧数上限，<=0 不限 |
| `MaxInflightBytes` | 4 MiB | 会话在途字节上限，<=0 不限 |
| `MaxOutboundBytes` | 2 MiB | 待写队列字节上限，<=0 不限 |
| `InflightPolicy` | Block | Reject=立即拒绝，Block=背压 |
| `ConcurrentHandlers` | false | true=每帧独立 goroutine（默认串行保序） |
| `EnableMux` | false | true=一条连接多路逻辑流（也可由 `SetStreamHandler` 置位） |
| `Mux.MaxStreams` | 64 | 每连接活跃逻辑流上限，<=0 不限 |
| `Mux.MaxStreamInflightFrames` | 64 | 单路在途帧上限，<=0 不限 |
| `Mux.MaxStreamInflightBytes` | 1 MiB | 单路在途字节上限，<=0 不限 |
| `Mux.MaxStreamOutboundBytes` | 1 MiB | 单路待写字节上限，<=0 不限 |
| `Mux.Policy` | Block | 跟随会话 `InflightPolicy`：Reject/Block |
| `IdleTimeout` | 0（关闭） | 双向空闲超时 |
| `GracePeriod` | 5s | 宽限排空时长 |
| `WriteTimeout` | 10s | 单次写超时，<=0 不设 |

### 网关 `gateway.Config`

`Listen`（默认 127.0.0.1:0 随机端口）、`MaxConnections`、`GlobalBytes`、
`Session`（会话选项）、`StreamHandler`（非 nil 即整条网关启用多路模式）、
`ShutdownGrace`（默认 5s）、`OnSessionEnd`（会话终结回调，可观测状态与原因）。
计数器见 `gateway.Stats()`：
`Accepted / RejectedConnQuota / RejectedWhileShutdown`。

## 6. 本地验证

### 6.1 运行全部测试（建议带 race）

```bash
go build ./...
go vet ./...
go test ./... -race -count=1
```

### 6.2 测试覆盖矩阵

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
| `mux.TestMultiStreamOrderAndConcurrency` | 多路交错投递 | 路内序号严格递增，跨路峰值并发>1 |
| `mux.TestSlowStreamDoesNotBlockOthers` | 一路卡住 | 慢路 worker 卡住时快路 5 帧仍按时回写 |
| `mux.TestStreamLevelQuotaReject` | 路级在途拒收 | 仅该路 aborted、连接不终止，其它路继续 |
| `mux.TestRejectErrorDimensions` | 拒收原因维度 | scope×dir 可 As 提取，wrap ErrQuotaRejected |
| `mux.TestStreamHalfCloseIndependent` | 单路半关闭 | 该路 Closed，stream2 仍收发，连接不半闭 |
| `mux.TestStreamQuotaReleasedAfterFinish` | 路配额回收 | MaxStreams=2 下连续收尾 3 路不泄漏 |
| `mux.TestGraceExpiryAbortsStuckStream` | 宽限到期一路卡住 | 好路收尾、卡路被 abort 且出现在 stuck 列表 |
| `mux.TestAbortAllReleasesEverything` | 连接强杀 | 忽略 ctx 处理器不阻塞，各级在途记账归零 |
| `session.TestMuxEndToEndConcurrencyEcho` | 真实 TCP 多路 echo | 5 路×12 帧按路按序返回无串包 |
| `session.TestMuxBadBytesInterleavedRecovery` | 坏字节穿插多路 | 3 类坏帧重同步后各路归属/顺序正确、好帧不丢 |
| `session.TestMuxSlowStreamRealBackpressure` | 真实连接跨路不阻塞 | 慢路卡住时快路 3 帧按时回显 |
| `session.TestMuxGraceShutdownStuckStream` | 停机宽限到期一路卡住 | code=3 强制终止、Wait 返回可解释错误、不被挂死 |
| `gateway.TestGatewayMultiplexedStreams` | 网关级多路集成 | 6 路×10 帧经真实端口按序回显，Shutdown 干净 |
| `gateway.TestConcurrentAccept` | 40 客户端并发 | 全部 echo 成功，Accepted=40 |
| `gateway.TestConnectionQuotaReject` | 连接数超限 | 10 个溢出连接全部无 echo 且计数正确 |
| `gateway.TestGlobalBytesQuotaReject` | 全局字节预算超限 | 会话以配额原因 code=13 终止 |
| `gateway.TestShutdownRace` | 8 路并发 Shutdown + 停机后拨号 | 无 panic、active=0、新连接被拒 |

单测日志统一打印“场景 / 连接状态 / 判定依据”，可用 `-v` 查看完整轨迹。

### 6.3 手工冒烟

单路模式：

```bash
go run ./cmd/demo -addr 127.0.0.1:9000 -idle 30s -grace 3s
```

多路模式（一条连接跑多路逻辑流，可调每连接活跃路数）：

```bash
go run ./cmd/demo -addr 127.0.0.1:9000 -mux -max-streams 64 -idle 30s -grace 3s
```

服务端打印监听地址后，用 Go 客户端 `frame.Encode` 在**同一条连接**上交替
发送不同 `StreamID` 的 V1/V2 帧即可收到按路回显：同一路帧严格保序，某一路
卡住不影响其它路；给某路发带 END_STREAM 的帧只半关闭该路；Ctrl+C 观察
宽限排空（到期仍卡住的路会被强制终止，原因含卡住流 ID）与最终 `stats`
输出。

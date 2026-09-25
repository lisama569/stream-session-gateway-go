# 本地双向流会话网关（Go）

一个基于本地 TCP 的双向流（bidirectional streaming）会话网关：多客户端可并发接入，
网关对多版本二进制帧进行解码、配额管理与生命周期控制。目标是在**异常链路**
（半包、粘包、长度越界、校验失败、未知版本、配额耗尽、空闲、停机竞态）下，
行为仍然**一致、可复现、可解释**——不静默丢弃、不串包、不重复、不泄漏内存。

- 模块路径：`github.com/lisama569/stream-session-gateway-go`
- 核心包：`gateway/`
- 可运行演示：`cmd/gateway-demo`

---

## 1. 协议兼容范围

所有整数均为大端序，每一帧以魔数 `0xA5` 开头。

### V1（Version=1）

```
magic(1)=0xA5 | ver(1)=1 | type(1) | length(2) | seq(4) | payload(length) | crc16(2)
```

- 载荷上限：`2^16-1`，并受配置 `MaxPayloadV1`（默认 16KiB）进一步约束；
- 校验：CRC-16/MODBUS（多项式 0xA001，初值 0xFFFF），覆盖 `type` 起到 CRC 前的全部字节。

### V2（Version=2）

```
magic(1)=0xA5 | ver(1)=2 | type(1) | flags(1) | length(4) | seq(4) | payload(length) | crc32(4)
```

- 载荷上限：`2^32-1`，并受配置 `MaxPayloadV2`（默认 1MiB）进一步约束；
- 校验：CRC-32（IEEE 802.3，`crc32.ChecksumIEEE`），覆盖 `type` 起到 CRC 前的全部字节；
- 较 V1 多出 `flags(1)` 与 32 位长度字段。

### 帧类型

| Type | 含义 |
| --- | --- |
| 1 `TypeData` | 业务数据（如 EchoHandler 原样回显） |
| 2 `TypeReject` | 拒绝回执：`flags` 携带可区分的 `ErrorKind`，payload 为原因字符串 |
| 3 `TypeClose` | 关闭通知：`flags` 携带关闭原因类别 |

> 协议规定 Reject/Close 等控制帧始终以 V2 编码（需要 flags 字段）；数据帧支持 V1/V2。

### 异常区分与恢复（不静默丢弃、不串包）

解码器（`Decoder`）是**有界流式**解码器，内部缓冲不超过最大合法帧长。`Feed` 同时返回
数据帧与 `[]*Error`，每类问题都有独立的 `ErrorKind`：

| 现象 | 错误类别 | 恢复行为 |
| --- | --- | --- |
| 半包 | 不报错 | 不完整帧留在缓冲，等待后续字节 |
| 粘包 | 不报错 | 一次返回多个帧，顺序不变 |
| 魔数前的杂字节 | `malformed_frame` | 显式报错后丢弃杂字节，重新同步 |
| 未知版本号 | `unknown_version` | 报错并跳过该两字节，继续找下一个魔数 |
| 声明长度超配置上限 | `length_exceeded` | 报错并跳过该坏帧，缓冲不随声明长度增长 |
| 校验失败 | `checksum_mismatch` | 报错并跳过该帧，后续好帧正常解出 |

底层网络错误一律包装为 `kind=conn_io` 的 `*gateway.Error`，不会把 `*net.OpError` 直接透出。
用 `gateway.AsError(err)` / `gateway.IsKind(err, kind)` 做稳定的程序化判断。

---

## 2. 配额与背压语义

配额分为两级，数据帧在**派发处理前**申请，处理结束后释放：

| 配额 | 作用域 | 拒绝类别 |
| --- | --- | --- |
| `GlobalConcurrent` | 全局并发会话数（接入时申请，会话结束释放） | `global_concurrent_limit` |
| `GlobalInflightBytes` | 全局在途载荷字节数 | `global_bytes_limit` |
| `SessionInflightFrames` | 单会话在途帧数 | `session_frames_limit` |
| `SessionInflightBytes` | 单会话在途载荷字节数 | `session_bytes_limit` |

申请顺序为「会话帧数 → 会话字节 → 全局字节」，任一失败会回滚已申请部分，不会泄漏配额。

策略（`Quotas.Policy`）：

- **`PolicyReject`（值 0）**：立即拒绝。服务端向对端回 `TypeReject` 帧，
  `flags` 即拒绝原因（如 `session_bytes_limit`），连接保持可用，后续帧不受影响。
  全局并发超限发生在接入阶段：新连接会先收到 Reject 再被关闭。
- **`PolicyBlock`（值 1，默认）**：排队阻塞，形成背压。阻塞点在读循环中，
  TCP 接收窗口随之反压发送方；信号量按 FIFO 唤醒，不允许后来的小请求插队，
  `context` 取消时不占用配额。

内存有界性保证：

- 解码器缓冲 ≤ 最大帧长；读缓冲大小固定（`ReadBuffer`，默认 4KiB）；
- 每会话在途帧数/字节数有硬上限，跨会话全局字节数有硬上限；
- 会话总数受 `GlobalConcurrent` 限制——内存占用不随连接数无界增长；
- 已成功解码的帧恰好派发给 Handler 一次（不丢失、不重复）。

---

## 3. 生命周期与关闭语义

会话状态：`active → draining → closing → closed`。

| 操作 | 语义 |
| --- | --- |
| 正常关闭 `Session.Close(reason)` | 尽力发送 `TypeClose`，关闭连接，进入 `closed`；**幂等**，重复/并发调用返回首次原因 |
| 半关闭 `Session.CloseRead(reason)` | 停止读入并进入 `draining`，**写侧继续可用**；已在途的帧继续处理与回写 |
| 空闲超时 `IdleTimeout` | 读静默超过阈值，以 `idle_timeout` 强制关闭并保留原因；0 表示不启用 |
| 优雅停机 `Gateway.Shutdown(ctx)` | ① 关闭监听器、拒绝新接入（`shutting_down`）② 半关闭所有会话读侧 ③ 在 `GracePeriod` 内排空在途帧 ④ 宽限到期仍未结束的会话被强制终止（`grace_expired`） |
| 强制终止 `ForceClose` / `ForceShutdown(reason)` | 立即关闭，不发送关闭帧；原因被保留为 `force_terminated` 且可解释 |

竞态保证：

- 停机窗口内的新连接：要么在监听器层被拒绝，要么收到 `Reject/shutting_down`，
  不存在“注册到一半”的会话；
- 重复 `Shutdown`/`ForceShutdown` 不崩溃、不挂起、状态不回退；
- 关闭与发送并发时，写操作以会话状态为闸，写连接由互斥锁串行化；
- Handler panic 会被恢复并记录，不会拖垮其它会话或网关进程。

---

## 4. 可配置项

`gateway.Config`（零值字段会继承默认值，见下表）：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `ListenAddr` | `127.0.0.1:0` | 本地监听地址（`:0` 由系统分配端口） |
| `MaxPayloadV1` | 16384 | V1 单帧载荷上限 |
| `MaxPayloadV2` | 1048576 | V2 单帧载荷上限 |
| `IdleTimeout` | 0（关闭） | 读空闲超时时间 |
| `GracePeriod` | 5s | 优雅停机宽限排空时间 |
| `ReadBuffer` | 4096 | 单次读连接的缓冲大小 |
| `Handler` | 必填 | 业务帧处理器（实现 `Handler`，或用 `HandlerFunc`/`EchoHandler`） |
| `Logger` | 静默 | 实现 `Printf` 的日志器；`gateway.NewLogger(io.Writer)` 可快速构造 |

`Quotas` 中整型配额：**正数=配额大小；0=显式不限制；负数（或直接用零值 Config）=用默认值**。

| 配额字段 | 默认值 |
| --- | --- |
| `GlobalConcurrent` | 64 |
| `GlobalInflightBytes` | 32 MiB |
| `SessionInflightFrames` | 1024 |
| `SessionInflightBytes` | 4 MiB |
| `Policy` | `PolicyBlock` |

---

## 5. 本地验证方法

需要 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 全部单元/集成测试（输出场景、连接状态与判定依据）
go test -count=1 -v ./...

# 竞态检测
go test -race -count=1 ./...

# 可运行演示：多客户端、V1/V2 混合、逐字节分片（半包）、优雅停机
go run ./cmd/gateway-demo -clients 5
# 可选：-addr 127.0.0.1:19090
```

### 测试场景一览

| 测试 | 覆盖点 |
| --- | --- |
| `TestCodecSmoke` / `TestDecoder*` | 半包、粘包、长度越界、CRC 失败、未知版本、重新同步、缓冲有界 |
| `TestLimiterReject` / `TestLimiterBlock` | 拒绝原因可区分、阻塞背压、FIFO、取消不泄漏配额 |
| `TestE2EEchoConcurrent` | 12 个并发客户端、V1/V2 混合、逐字节发送 |
| `TestE2EGlobalConcurrentReject` | 全局并发超限 → Reject/global_concurrent_limit |
| `TestE2ESessionBytesQuotaReject` | 会话字节超限拒绝后链路恢复，已接收帧不丢不重 |
| `TestE2EBackpressureNoLossNoDup` | 阻塞背压下帧恰好处理一次 |
| `TestE2EGlobalBytesBackpressure` | 跨会话全局字节背压 |
| `TestE2EProtocolRecovery` | 真实链路上的协议错误恢复，不串包 |
| `TestE2EGracefulShutdownDrain` | 半关闭 + 宽限排空，在途帧不丢失 |
| `TestE2EGraceExpiredForceTerminate` | 宽限到期强制终止，原因=grace_expired |
| `TestE2ENewConnDuringShutdownRejected` | 停机竞态：新接入拒绝、重复关闭安全 |
| `TestE2EIdleTimeout` | 空闲超时按预期关闭连接 |

### 最小用法

```go
g, _ := gateway.New(gateway.Config{
    ListenAddr:  "127.0.0.1:9000",
    GracePeriod: 5 * time.Second,
    Handler:     gateway.EchoHandler{},
    Logger:      gateway.NewLogger(os.Stdout),
})
go g.ListenAndServe(ctx)
// ...
g.Shutdown(context.Background())           // 优雅停机（排空）
// 或 g.ForceShutdown("operator triggered") // 立即强制终止
```

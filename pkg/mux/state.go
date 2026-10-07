package mux

import "fmt"

// StreamState 单路逻辑流的生命周期状态（只进不退）。
//
// 合法迁移：
//
//	Active ──本端CloseWrite──▶ LocalHalf ──对端END_STREAM──▶ BothHalf ──排空──▶ Closed
//	Active ──对端END_STREAM──▶ RemoteHalf ──本端CloseWrite─▶ BothHalf ──排空──▶ Closed
//	任意状态 ──Abort/连接强制终止──▶ Aborted
//
// Closed 与 Aborted 为吸收态，不回退；重复终止幂等，保留首次原因。
type StreamState uint8

const (
	StreamActive     StreamState = iota
	StreamLocalHalf              // 本端已半关闭写侧
	StreamRemoteHalf             // 对端已 END_STREAM
	StreamBothHalf               // 双向半闭，等待在途排空
	StreamClosed                 // 正常收尾（吸收态）
	StreamAborted                // 异常终止（吸收态）
)

// String 返回状态名。
func (s StreamState) String() string {
	names := [...]string{"active", "local_half", "remote_half", "both_half", "closed", "aborted"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("stream_state(%d)", int(s))
}

// terminal 报告是否为吸收态。
func (s StreamState) terminal() bool { return s == StreamClosed || s == StreamAborted }

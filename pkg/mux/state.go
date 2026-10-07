// Package mux 在单条双向帧连接之上提供多路逻辑流复用。
//
// 设计要点（骨架，实现逐文件补全）：
//   - 解码读循环 / 分派路由 / 每路串行 worker / 单写循环彼此解耦，
//     某一路处理慢、卡住或不结束，不阻塞其它路的收发；
//   - 每一路有独立在途/待写配额，连接级有总在途/待写/活跃路数上限；
//   - 每一路可独立半关闭/重置，不影响同连接其它路；
//   - 关闭与停机沿用宽限状态机：宽限内排空，到点强制终止并给出原因。
package mux

import "fmt"

// State 多路连接的生命周期状态（迁移语义与 session.State 对齐）。
type State uint8

const (
	StateActive State = iota
	StateLocalHalf
	StateRemoteHalf
	StateClosing
	StateClosed
	StateForceTerminated
)

func (s State) String() string {
	names := [...]string{"active", "local_half", "remote_half", "closing", "closed", "force_terminated"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("state(%d)", int(s))
}

// terminal 报告状态是否为吸收态。
func (s State) terminal() bool { return s == StateClosed || s == StateForceTerminated }

// streamState 是单条逻辑流的生命周期状态。
type streamState uint8

const (
	streamActive     streamState = iota // 双向活跃
	streamLocalHalf                     // 本端已 END_STREAM（不再能写）
	streamRemoteHalf                    // 对端已 END_STREAM（不会再有入帧）
	streamClosed                        // 双方收尾完成（吸收态）
	streamReset                         // 任一端 RST 强制结束（吸收态）
)

func (s streamState) String() string {
	names := [...]string{"active", "local_half", "remote_half", "closed", "reset"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("stream_state(%d)", int(s))
}

// terminal 报告流状态是否为吸收态。
func (s streamState) terminal() bool { return s == streamClosed || s == streamReset }

// CloseReason 解释连接/流为何终止，字段与 session.CloseReason 对齐。
type CloseReason struct {
	Code   uint16
	Text   string
	Forced bool
}

func (r CloseReason) String() string {
	kind := "graceful"
	if r.Forced {
		kind = "forced"
	}
	return fmt.Sprintf("close[%s code=%d]: %s", kind, r.Code, r.Text)
}

// 预定义关闭原因（与 session 包编码保持一致，扩展多路语义在 20+ 段）。
var (
	ReasonNormal          = CloseReason{Code: 0, Text: "normal close"}
	ReasonPeerEndStream   = CloseReason{Code: 1, Text: "peer end stream"}
	ReasonIdleTimeout     = CloseReason{Code: 2, Text: "idle timeout", Forced: true}
	ReasonGraceExpired    = CloseReason{Code: 3, Text: "grace drain expired", Forced: true}
	ReasonGatewayDown     = CloseReason{Code: 4, Text: "gateway shutting down"}
	ReasonForcedByCaller  = CloseReason{Code: 10, Text: "forced by caller", Forced: true}
	ReasonProtocolError   = CloseReason{Code: 11, Text: "protocol error", Forced: true}
	ReasonIOFailure       = CloseReason{Code: 12, Text: "io failure", Forced: true}
	ReasonQuotaReject     = CloseReason{Code: 13, Text: "quota rejected", Forced: true}
	ReasonStreamReset     = CloseReason{Code: 20, Text: "stream reset by peer", Forced: true}
	ReasonTooManyStreams  = CloseReason{Code: 21, Text: "too many active streams", Forced: false}
	ReasonStreamQuota     = CloseReason{Code: 22, Text: "stream quota exceeded", Forced: false}
	ReasonInvalidStreamID = CloseReason{Code: 23, Text: "invalid stream id", Forced: true}
)

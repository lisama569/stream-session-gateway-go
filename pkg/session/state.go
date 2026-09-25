package session

import "fmt"

// State 会话生命周期状态。
//
// 合法迁移：
//
//	Active ──CloseWrite──▶ LocalHalf ──对端EndStream──▶ Closed(双方半闭=完整关闭)
//	Active ──收END_STREAM─▶ RemoteHalf ──CloseWrite───▶ Closed
//	Active/LocalHalf/RemoteHalf ──GracefulClose──▶ Closing ──排空完成/超时──▶ Closed/ForceTerminated
//	任意状态 ──ForceClose──▶ ForceTerminated
//
// Closed 与 ForceTerminated 为吸收态，不再回退；重复关闭幂等并保留首次原因。
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

// CloseReason 以代码+原因文本解释会话为何终止，Forced 标记强制终止。
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

// 预定义关闭原因。
var (
	ReasonNormal         = CloseReason{Code: 0, Text: "normal close"}
	ReasonPeerEndStream  = CloseReason{Code: 1, Text: "peer end stream"}
	ReasonIdleTimeout    = CloseReason{Code: 2, Text: "idle timeout", Forced: true}
	ReasonGraceExpired   = CloseReason{Code: 3, Text: "grace drain expired", Forced: true}
	ReasonGatewayDown    = CloseReason{Code: 4, Text: "gateway shutting down"}
	ReasonForcedByCaller = CloseReason{Code: 10, Text: "forced by caller", Forced: true}
	ReasonProtocolError  = CloseReason{Code: 11, Text: "protocol error", Forced: true}
	ReasonIOFailure      = CloseReason{Code: 12, Text: "io failure", Forced: true}
	ReasonQuotaReject    = CloseReason{Code: 13, Text: "quota rejected", Forced: true}
)

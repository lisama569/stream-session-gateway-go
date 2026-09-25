package gateway

// Version 标识帧协议版本。
type Version uint8

const (
	V1 Version = 1
	V2 Version = 2
)

// Magic 是每一帧开头的同步魔数字节。
// 选择 0xA5（二进制 10100101）便于在乱码流中重新定位帧边界。
const Magic byte = 0xA5

// FrameType 是帧的业务类型。
type FrameType uint8

const (
	TypeData   FrameType = 1 // 业务数据
	TypeReject FrameType = 2 // 配额拒绝回执
	TypeClose  FrameType = 3 // 关闭通知
)

// 帧的线格式（全部整数均为大端序）：
//
//	V1: magic(1)=0xA5 | ver(1)=1 | type(1) | length(2) | seq(4) |
//	    payload(length) | crc16(2)
//	V2: magic(1)=0xA5 | ver(1)=2 | type(1) | flags(1) | length(4) | seq(4) |
//	    payload(length) | crc32(4)
//
// V1 载荷上限 1<<16-1；V2 载荷上限 1<<32-1，但受 Codec 中配置的
// 更小的 maxPayload 限制。CRC 的计算范围：V1 覆盖 type 起、CRC 之前
// 的全部字节；V2 同样覆盖 CRC 之前的全部字节（不含 magic 与 ver）。
const (
	v1HeaderLen  = 9 // magic+ver+type+len(2)+seq(4)
	v1LenOff     = 3 // length 字段起始偏移
	v1SeqOff     = 5 // seq 字段起始偏移
	v1CrcLen     = 2
	v1MaxWireLen = 1<<16 - 1

	v2Prefix     = 4 // magic+ver+type+flags
	v2LenOff     = 4 // length 字段起始偏移（紧跟 flags）
	v2SeqOff     = 8
	v2HeaderLen  = 12 // magic..seq（不含 payload、crc）
	v2CrcLen     = 4
	v2MaxWireLen = 1<<32 - 1
)

// Frame 是与协议版本无关的帧抽象。
// V1 没有 Flags 字段，编码时按 0 处理；V2 携带 1 字节 Flags。
type Frame struct {
	Version Version
	Type    FrameType
	Flags   uint8
	Seq     uint32
	Payload []byte
}

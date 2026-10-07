// Package frame 定义多版本二进制帧协议的编解码原语。
//
// 统一帧布局（V1/V2 头部相同，校验算法不同）:
//
//	偏移 0:     SOF 固定 0xAA，用于字节流重同步
//	偏移 1:     版本 (1=V1, 2=V2)
//	偏移 2:     标志位（bit0: END_STREAM 半关闭）
//	偏移 3-4:   PayloadLen 大端（最大 1<<20）
//	偏移 5-8:   StreamID 大端
//	偏移 9..:   Payload
//	V1: 末尾 1 字节负载逐字节异或校验
//	V2: 末尾 2 字节 CRC-16/CCITT-FALSE（多项式 0x1021，初始 0xFFFF）
package frame

import "errors"

// Version 帧协议版本。
type Version uint8

const (
	V1 Version = 1
	V2 Version = 2
)

// 标志位。
const (
	FlagEndStream uint8 = 0x01
	// FlagReset 表示 RST_STREAM：StreamID 字段给出被重置的路，
	// 负载为可选的 UTF-8 原因文本。仅多路复用（pkg/mux）语义使用。
	FlagReset uint8 = 0x02
)

// 协议常量。
const (
	SOF           byte = 0xAA
	HeaderLen          = 9
	MaxPayloadLen      = 1 << 15 // 32KiB，协议级单帧负载上限
)

// Frame 是一帧应用数据的内存表示。
type Frame struct {
	Version Version
	Flags   uint8
	Stream  uint32
	Payload []byte
}

// EndStream 返回该帧是否携带半关闭标志。
func (f *Frame) EndStream() bool { return f.Flags&FlagEndStream != 0 }

// 帧级错误，全部可被 errors.Is 区分。
var (
	ErrUnknownVersion = errors.New("frame: unknown protocol version")
	ErrLengthExceeded = errors.New("frame: payload length out of bound")
	ErrChecksum       = errors.New("frame: checksum mismatch")
	ErrResync         = errors.New("frame: resynchronized to next SOF")
)

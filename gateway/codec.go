package gateway

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	defaultMaxPayloadV1 = 1 << 14 // 16KiB
	defaultMaxPayloadV2 = 1 << 20 // 1MiB
)

// crc16Table 是 CRC-16/MODBUS 的查表（多项式 0xA001，初值 0xFFFF）。
var crc16Table [256]uint16

func init() {
	for i := 0; i < 256; i++ {
		crc := uint16(i)
		for j := 0; j < 8; j++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
		crc16Table[i] = crc
	}
}

func crc16(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc = (crc >> 8) ^ crc16Table[(crc^uint16(b))&0xFF]
	}
	return crc
}

// Codec 负责帧的编码，并持有各版本允许的最大载荷长度。
type Codec struct {
	maxPayload map[Version]int
	// maxFrameSize 是任何合法帧在线上的最大字节数，用于约束解码缓冲。
	maxFrameSize int
}

// NewCodec 创建编解码器；0 表示使用内置默认上限。
func NewCodec(maxPayloadV1, maxPayloadV2 int) *Codec {
	if maxPayloadV1 <= 0 {
		maxPayloadV1 = defaultMaxPayloadV1
	}
	if maxPayloadV2 <= 0 {
		maxPayloadV2 = defaultMaxPayloadV2
	}
	if maxPayloadV1 > v1MaxWireLen {
		maxPayloadV1 = v1MaxWireLen
	}
	if maxPayloadV2 > v2MaxWireLen {
		maxPayloadV2 = v2MaxWireLen
	}
	c := &Codec{maxPayload: map[Version]int{V1: maxPayloadV1, V2: maxPayloadV2}}
	c.maxFrameSize = c.frameSize(V2, maxPayloadV2)
	return c
}

// frameSize 返回指定版本某载荷长度对应的整帧字节数。
func (c *Codec) frameSize(v Version, payload int) int {
	switch v {
	case V1:
		// magic..seq 共 v1HeaderLen，加 payload 与 crc16(2)
		return v1HeaderLen + payload + v1CrcLen
	case V2:
		return v2HeaderLen + payload + v2CrcLen
	default:
		return 0
	}
}

// Supported 报告版本是否受支持。
func (c *Codec) Supported(v Version) bool {
	_, ok := c.maxPayload[v]
	return ok
}

// MaxPayload 返回指定版本的最大载荷长度，未知版本返回 0。
func (c *Codec) MaxPayload(v Version) int { return c.maxPayload[v] }

// Encode 将一帧编码为完整字节切片。
func (c *Codec) Encode(f *Frame) ([]byte, error) {
	p := len(f.Payload)
	switch f.Version {
	case V1:
		if p > c.maxPayload[V1] {
			return nil, newError(KindPayloadTooLarge, "encode",
				"v1 payload exceeds configured limit", nil)
		}
		buf := make([]byte, c.frameSize(V1, p))
		buf[0] = Magic
		buf[1] = byte(V1)
		buf[2] = byte(f.Type)
		binary.BigEndian.PutUint16(buf[v1LenOff:v1LenOff+2], uint16(p))
		binary.BigEndian.PutUint32(buf[v1SeqOff:v1SeqOff+4], f.Seq)
		copy(buf[v1HeaderLen:], f.Payload)
		crcOff := len(buf) - v1CrcLen
		// CRC 覆盖 type 起到 CRC 之前。
		binary.BigEndian.PutUint16(buf[crcOff:], crc16(buf[2:crcOff]))
		return buf, nil
	case V2:
		if p > c.maxPayload[V2] {
			return nil, newError(KindPayloadTooLarge, "encode",
				"v2 payload exceeds configured limit", nil)
		}
		buf := make([]byte, c.frameSize(V2, p))
		buf[0] = Magic
		buf[1] = byte(V2)
		buf[2] = byte(f.Type)
		buf[3] = f.Flags
		binary.BigEndian.PutUint32(buf[v2LenOff:v2LenOff+4], uint32(p))
		binary.BigEndian.PutUint32(buf[v2SeqOff:v2SeqOff+4], f.Seq)
		copy(buf[v2HeaderLen:], f.Payload)
		crcOff := len(buf) - v2CrcLen
		binary.BigEndian.PutUint32(buf[crcOff:],
			crc32.ChecksumIEEE(buf[2:crcOff]))
		return buf, nil
	default:
		return nil, newError(KindUnknownVersion, "encode",
			"unknown frame version", nil)
	}
}

// WriteFrame 将一帧完整写入 w。
// 若多个 goroutine 可能并发写同一个 w，调用方应自行串行化（Session 已保证）。
func (c *Codec) WriteFrame(w io.Writer, f *Frame) error {
	buf, err := c.Encode(f)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	if err != nil {
		return newError(KindConnIO, "write", "short/conn write failure", err)
	}
	return nil
}

// Decoder 是有界的流式帧解码器。
// 内部缓冲容量受最大帧长约束，不会随输入无界增长。
type Decoder struct {
	codec *Codec
	buf   []byte
	off   int // 尚未处理数据的起点
}

// NewDecoder 创建绑定到 Codec 的解码器。
func NewDecoder(c *Codec) *Decoder {
	return &Decoder{codec: c, buf: make([]byte, 0, 1024)}
}

// Buffered 返回内部缓冲中尚未成帧的字节数。
func (d *Decoder) Buffered() int { return len(d.buf) - d.off }

// compact 把尚未处理的数据搬回缓冲开头，丢弃已消费部分。
func (d *Decoder) compact() {
	if d.off == 0 {
		return
	}
	n := copy(d.buf, d.buf[d.off:])
	d.buf = d.buf[:n]
	d.off = 0
}

// Feed 喂入新读到的字节，返回本次可成帧的数据帧与可恢复的解码错误。
//
// 语义：
//   - 半包：不完整的帧留在内部缓冲等待后续 Feed；
//   - 粘包：一次 Feed 中的多个帧全部解出，顺序不变；
//   - 未知版本 / 长度越界 / 校验失败 / 帧损坏：以 *Error 分别返回，
//     随后自动重新同步到下一个 Magic，绝不静默丢弃，也不会串包；
//   - 缓冲受 maxFrameSize 约束，无法成帧的垃圾数据被显式报错后丢弃。
func (d *Decoder) Feed(chunk []byte) (frames []*Frame, errs []*Error) {
	d.compact()
	d.buf = append(d.buf, chunk...)

	for d.Buffered() > 0 {
		b := d.buf[d.off:]

		// 1) 定位 Magic。它之前的字节都属于无法解释的垃圾数据。
		if b[0] != Magic {
			next := indexMagic(b)
			if next < 0 {
				// 全部没有 Magic：若已超过最大帧长，报错丢弃；
				// 否则可能是半包（Magic 尚未到达），保留。
				if len(b) > d.codec.maxFrameSize {
					errs = append(errs, newError(KindMalformedFrame, "decode",
						"no frame magic found within max frame size, discarding bytes", nil))
					d.off = len(d.buf)
				}
				break
			}
			if next > 0 {
				errs = append(errs, newError(KindMalformedFrame, "decode",
					"resync: discarding non-magic leading bytes", nil))
				d.off += next
				b = d.buf[d.off:]
			}
		}

		// 2) 至少需要 magic+ver 才能决定帧长。
		if len(b) < 2 {
			break // 半包
		}
		ver := Version(b[1])
		if !d.codec.Supported(ver) {
			// 未知版本：无法知道帧长，丢弃 Magic+ver 两个字节后重新同步。
			errs = append(errs, newError(KindUnknownVersion, "decode",
				"unknown protocol version byte", nil))
			d.off += 2
			continue
		}

		switch ver {
		case V1:
			f, consumed, perr := d.decodeV1(b)
			if perr != nil {
				errs = append(errs, perr)
			}
			if f != nil {
				frames = append(frames, f)
			}
			if consumed == 0 {
				break // 半包：等待更多字节
			}
			d.off += consumed
			if perr != nil && f == nil {
				// 坏帧：只前进 1 字节（跳过 Magic）以便搜索真正的下一帧。
				// consumed 在坏帧时被置为 1。
			}
			continue
		case V2:
			f, consumed, perr := d.decodeV2(b)
			if perr != nil {
				errs = append(errs, perr)
			}
			if f != nil {
				frames = append(frames, f)
			}
			if consumed == 0 {
				break
			}
			d.off += consumed
			continue
		}
		break
	}

	if d.off > 0 {
		d.compact()
	}
	return frames, errs
}

func indexMagic(b []byte) int {
	for i, c := range b {
		if c == Magic {
			return i
		}
	}
	return -1
}

// decodeV1 解析 V1 帧。
// 返回值：f 为成功解出的帧；consumed 为本帧（或坏帧后跳步）消耗的字节数，
// 0 表示半包需要等待；perr 为可恢复的解码错误。
func (d *Decoder) decodeV1(b []byte) (f *Frame, consumed int, perr *Error) {
	// 头部需要 magic..seq 共 v1HeaderLen 字节。
	if len(b) < v1HeaderLen {
		return nil, 0, nil
	}
	plen := int(binary.BigEndian.Uint16(b[v1LenOff : v1LenOff+2]))
	total := v1HeaderLen + plen + v1CrcLen
	if plen > d.codec.maxPayload[V1] {
		return nil, 1, newError(KindLengthExceeded, "decode",
			"v1 declared length exceeds configured limit", nil)
	}
	if len(b) < total {
		return nil, 0, nil // 半包
	}
	crcOff := total - v1CrcLen
	got := binary.BigEndian.Uint16(b[crcOff:])
	want := crc16(b[2:crcOff])
	if got != want {
		return nil, 1, newError(KindChecksumMismatch, "decode",
			"v1 crc16 mismatch", nil)
	}
	return &Frame{
		Version: V1,
		Type:    FrameType(b[2]),
		Seq:     binary.BigEndian.Uint32(b[v1SeqOff : v1SeqOff+4]),
		Payload: append([]byte(nil), b[v1HeaderLen:crcOff]...),
	}, total, nil
}

// decodeV2 解析 V2 帧。
func (d *Decoder) decodeV2(b []byte) (f *Frame, consumed int, perr *Error) {
	if len(b) < v2HeaderLen {
		return nil, 0, nil
	}
	plen := int(binary.BigEndian.Uint32(b[v2LenOff : v2LenOff+4]))
	if plen > d.codec.maxPayload[V2] {
		return nil, 1, newError(KindLengthExceeded, "decode",
			"v2 declared length exceeds configured limit", nil)
	}
	total := v2HeaderLen + plen + v2CrcLen
	// 防御：声明长度即便不超配置，也不应让缓冲无界等待。
	if total > d.codec.maxFrameSize {
		return nil, 1, newError(KindLengthExceeded, "decode",
			"declared frame exceeds decoder bound", nil)
	}
	if len(b) < total {
		return nil, 0, nil
	}
	crcOff := total - v2CrcLen
	got := binary.BigEndian.Uint32(b[crcOff:])
	want := crc32.ChecksumIEEE(b[2:crcOff])
	if got != want {
		return nil, 1, newError(KindChecksumMismatch, "decode",
			"v2 crc32 mismatch", nil)
	}
	return &Frame{
		Version: V2,
		Type:    FrameType(b[2]),
		Flags:   b[3],
		Seq:     binary.BigEndian.Uint32(b[v2SeqOff : v2SeqOff+4]),
		Payload: append([]byte(nil), b[v2HeaderLen:crcOff]...),
	}, total, nil
}

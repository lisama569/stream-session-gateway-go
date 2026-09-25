package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// checksumLen 返回某版本帧尾校验字节数。
func checksumLen(v Version) int {
	switch v {
	case V1:
		return 1
	case V2:
		return 2
	default:
		return 0
	}
}

// maxResidual 是解码器在“尚未定位到任何帧头”时允许累积的最大垃圾字节，
// 防止对端灌入无 SOF 数据时内存无界增长。
const maxResidual = 1 << 20

func encodeCheck(f *Frame) (int, error) {
	if f.Version != V1 && f.Version != V2 {
		return 0, fmt.Errorf("%w: version=%d", ErrUnknownVersion, uint8(f.Version))
	}
	if len(f.Payload) > MaxPayloadLen {
		return 0, fmt.Errorf("%w: version=%d length=%d max=%d",
			ErrLengthExceeded, uint8(f.Version), len(f.Payload), MaxPayloadLen)
	}
	return HeaderLen + len(f.Payload) + checksumLen(f.Version), nil
}

func writeFrame(dst []byte, f *Frame) {
	dst[0] = SOF
	dst[1] = byte(f.Version)
	dst[2] = f.Flags
	binary.BigEndian.PutUint16(dst[3:5], uint16(len(f.Payload)))
	binary.BigEndian.PutUint32(dst[5:9], f.Stream)
	copy(dst[HeaderLen:], f.Payload)
	off := HeaderLen + len(f.Payload)
	switch f.Version {
	case V1:
		dst[off] = xorChecksum(f.Payload)
	case V2:
		binary.BigEndian.PutUint16(dst[off:off+2], crc16CCITT(f.Payload))
	}
}

// Append 把帧追加到 buf 末尾，返回新缓冲。
func Append(buf []byte, f *Frame) ([]byte, error) {
	n, err := encodeCheck(f)
	if err != nil {
		return buf, err
	}
	start := len(buf)
	buf = append(buf, make([]byte, n)...)
	writeFrame(buf[start:start+n], f)
	return buf, nil
}

// Encode 将帧完整序列化到 w；成功返回写出字节数。
// 任何参数错误都在写之前返回，绝不写出半截帧。
func Encode(w io.Writer, f *Frame) (int, error) {
	n, err := encodeCheck(f)
	if err != nil {
		return 0, err
	}
	buf := make([]byte, n)
	writeFrame(buf, f)
	// io.Writer 允许短写，用 WriteFull 语义保证要么完整写出要么报错。
	written := 0
	for written < n {
		m, werr := w.Write(buf[written:])
		written += m
		if werr != nil {
			return written, werr
		}
		if m == 0 {
			return written, io.ErrShortWrite
		}
	}
	return n, nil
}

// Decoder 是面向字节流的增量帧解码器。
type Decoder struct {
	buf []byte
	r   io.Reader
}

// NewDecoder 创建从 r 读取的解码器。
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// resyncToSOF 丢弃第一个 SOF 之前（含对齐逻辑）的垃圾字节。
// 返回 true 表示 buf 首字节已是 SOF；false 表示缓冲为空。
func (d *Decoder) discardToSOF() {
	for len(d.buf) > 0 && d.buf[0] != SOF {
		// 找到下一个候选 SOF。
		idx := -1
		for i := 1; i < len(d.buf); i++ {
			if d.buf[i] == SOF {
				idx = i
				break
			}
		}
		if idx < 0 {
			d.buf = d.buf[:0]
			return
		}
		d.buf = d.buf[idx:]
	}
}

// readMore 从底层 Reader 再读一批数据到缓冲，返回读取错误。
// 返回 io.EOF 是自然结束。
func (d *Decoder) readMore() error {
	if cap(d.buf)-len(d.buf) < 512 {
		grown := make([]byte, len(d.buf), cap(d.buf)*2+512)
		copy(grown, d.buf)
		d.buf = grown
	}
	tmp := d.buf[len(d.buf):cap(d.buf)]
	n, err := d.r.Read(tmp)
	d.buf = d.buf[:len(d.buf)+n]
	return err
}

// Next 返回下一帧。语义见类型注释：坏帧自动重同步，调用方可继续读取。
func (d *Decoder) Next(out *Frame) error {
	for {
		// 1) 对齐到候选 SOF。
		d.discardToSOF()

		// 2) 尝试从现有缓冲解析；不足则继续读。
		for {
			if f, perr := d.parseOne(); perr != errNeedMore {
				if perr == nil {
					*out = *f
					return nil
				}
				return perr // 坏帧错误：已重同步，外层可继续 Next
			}
			// 需要更多字节。
			if len(d.buf) > maxResidual {
				return fmt.Errorf("%w: buffered garbage exceeds %d bytes without a frame",
					ErrLengthExceeded, maxResidual)
			}
			rerr := d.readMore()
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					// 流结束：缓冲里只剩 SOF 或不足一帧的残余，属于截断。
					if len(d.buf) > 0 {
						d.buf = d.buf[:0]
						return io.ErrUnexpectedEOF
					}
					return io.EOF
				}
				return rerr // 底层 I/O 错误原样返回给上层决定终止
			}
		}
	}
}

var errNeedMore = errors.New("need more bytes")

// parseOne 假设 buf[0]==SOF，尝试解析一帧。
// 返回 errNeedMore 表示字节不足（不消费任何字节）。
// 返回坏帧错误时已把缓冲推进到下一候选 SOF（完成重同步）。
// 成功时返回帧（Payload 指向解码缓冲的独立副本），并消费整帧。
func (d *Decoder) parseOne() (*Frame, error) {
	b := d.buf
	if len(b) < HeaderLen {
		return nil, errNeedMore
	}
	v := Version(b[1])
	if v != V1 && v != V2 {
		// 未知版本：跳过当前候选 SOF，对齐下一个 SOF。
		d.resyncAfterBadFrame()
		return nil, fmt.Errorf("%w: version=%d at offset", ErrUnknownVersion, uint8(b[1]))
	}
	plen := int(binary.BigEndian.Uint16(b[3:5]))
	if plen > MaxPayloadLen {
		d.resyncAfterBadFrame()
		return nil, fmt.Errorf("%w: declared length=%d max=%d", ErrLengthExceeded, plen, MaxPayloadLen)
	}
	clen := checksumLen(v)
	total := HeaderLen + plen + clen
	if len(b) < total {
		return nil, errNeedMore // 半包：等待更多字节，保留 SOF
	}
	payload := b[HeaderLen : HeaderLen+plen]
	var checkOK bool
	switch v {
	case V1:
		checkOK = b[HeaderLen+plen] == xorChecksum(payload)
	case V2:
		got := binary.BigEndian.Uint16(b[HeaderLen+plen : HeaderLen+plen+2])
		checkOK = got == crc16CCITT(payload)
	}
	if !checkOK {
		d.resyncAfterBadFrame()
		return nil, fmt.Errorf("%w: version=%d length=%d", ErrChecksum, uint8(v), plen)
	}
	// 成功：拷贝 Payload 后再消费，避免与后续读缓冲别名。
	payloadCopy := make([]byte, plen)
	copy(payloadCopy, payload)
	f := &Frame{
		Version: v,
		Flags:   b[2],
		Stream:  binary.BigEndian.Uint32(b[5:9]),
		Payload: payloadCopy,
	}
	d.consume(total)
	return f, nil
}

// resyncAfterBadFrame 在坏帧之后把缓冲起点推进到“帧内可能的下一个 SOF”。
// 由于长度字段不可信，统一策略：丢弃当前 SOF，再对齐到下一个 SOF。
func (d *Decoder) resyncAfterBadFrame() {
	if len(d.buf) == 0 {
		return
	}
	d.buf = d.buf[1:] // 丢弃当前 SOF
	d.discardToSOF()
}

// consume 移动残余缓冲并压缩，防止 cap 无界增长。
func (d *Decoder) consume(n int) {
	rest := copy(d.buf, d.buf[n:])
	d.buf = d.buf[:rest]
}

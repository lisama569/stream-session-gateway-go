package frame

import (
	"bytes"
	"errors"
	"io"
	"log"
	"testing"
)

// chunkReader 模拟任意分片：按 sizes 逐块切分底层数据，
// sizes 循环使用；用于制造极端半包。
type chunkReader struct {
	data  []byte
	sizes []int
	pos   int
	si    int
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := c.sizes[c.si%len(c.sizes)]
	c.si++
	if n < 1 {
		n = 1
	}
	if n > len(p) {
		n = len(p)
	}
	if n > len(c.data)-c.pos {
		n = len(c.data) - c.pos
	}
	copy(p, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

func buildBytes(t *testing.T, fs ...*Frame) []byte {
	t.Helper()
	var buf []byte
	var err error
	for _, f := range fs {
		buf, err = Append(buf, f)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return buf
}

func TestRoundTripV1V2(t *testing.T) {
	fs := []*Frame{
		{Version: V1, Stream: 1, Payload: []byte("hello")},
		{Version: V2, Stream: 2, Flags: FlagEndStream, Payload: []byte("world!!")},
		{Version: V2, Stream: 3, Payload: []byte{}},
	}
	data := buildBytes(t, fs...)
	dec := NewDecoder(bytes.NewReader(data))
	var got Frame
	for i, want := range fs {
		if err := dec.Next(&got); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if got.Version != want.Version || got.Stream != want.Stream ||
			!bytes.Equal(got.Payload, want.Payload) || got.Flags != want.Flags {
			t.Fatalf("frame %d mismatch: %+v vs %+v", i, got, want)
		}
	}
	if err := dec.Next(&got); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestSplitAndSticky(t *testing.T) {
	// 三帧粘成一条字节流，再按 1,1,2,3,5 字节逐片喂入（极端半包）。
	data := buildBytes(t,
		&Frame{Version: V1, Stream: 7, Payload: []byte("abc")},
		&Frame{Version: V2, Stream: 8, Payload: make([]byte, 300)},
		&Frame{Version: V1, Stream: 9, Payload: []byte("xyz")},
	)
	dec := NewDecoder(&chunkReader{data: data, sizes: []int{1, 1, 2, 3, 5}})
	var got Frame
	count := 0
	for {
		err := dec.Next(&got)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected err at frame %d: %v", count, err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("want 3 frames, got %d", count)
	}
}

// corruptReader 在每两帧之间插入坏数据，验证坏帧后可恢复且不串包。
func TestCorruptRecovery(t *testing.T) {
	good1 := buildBytes(t, &Frame{Version: V1, Stream: 100, Payload: []byte("g1")})
	good2 := buildBytes(t, &Frame{Version: V2, Stream: 200, Payload: []byte("g2")})
	good3 := buildBytes(t, &Frame{Version: V1, Stream: 300, Payload: []byte("g3")})

	// 坏块1：SOF + 未知版本 9
	badVer := []byte{SOF, 9, 0, 0, 1, 0, 0, 0, 0, 'X', 0}
	// 坏块2：SOF + V1 声明长度 100，但实际给 2 字节后立刻给正确的 SOF 帧
	//        —— 校验失败/重同步应跨过坏数据。
	f1 := buildBytes(t, &Frame{Version: V1, Stream: 1, Payload: []byte{'A'}})
	badCsum := make([]byte, len(f1))
	copy(badCsum, f1)
	badCsum[len(badCsum)-1] ^= 0xFF // 翻转校验字节
	// 长度越界：length 字段超过 MaxPayloadLen
	badLen := []byte{SOF, 1, 0, 0xFF, 0xFF, 0, 0, 0, 0}

	var stream []byte
	stream = append(stream, good1...)
	stream = append(stream, badVer...)
	stream = append(stream, badCsum...)
	stream = append(stream, badLen...)
	stream = append(stream, good2...)
	stream = append(stream, good3...)

	dec := NewDecoder(bytes.NewReader(stream))
	var got Frame
	var good []uint32
	bad := map[error]int{}
	for {
		err := dec.Next(&got)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			switch {
			case errors.Is(err, ErrUnknownVersion):
				bad[ErrUnknownVersion]++
			case errors.Is(err, ErrChecksum):
				bad[ErrChecksum]++
			case errors.Is(err, ErrLengthExceeded):
				bad[ErrLengthExceeded]++
			default:
				t.Fatalf("unexpected error type: %v", err)
			}
			continue
		}
		good = append(good, got.Stream)
	}
	log.Printf("场景=坏帧恢复 好帧流=%v 坏帧分类=%v 判定依据=三类哨兵错误各被识别且好帧全部回收", good, bad)
	if len(good) != 3 || good[0] != 100 || good[1] != 200 || good[2] != 300 {
		t.Fatalf("good frames lost/reordered: %v", good)
	}
	if bad[ErrUnknownVersion] < 1 || bad[ErrChecksum] < 1 || bad[ErrLengthExceeded] < 1 {
		t.Fatalf("each corruption must be reported distinctly, got %v", bad)
	}
}

func TestTruncatedTail(t *testing.T) {
	good := buildBytes(t, &Frame{Version: V1, Stream: 1, Payload: []byte("ok")})
	trunc := []byte{SOF, 1, 0, 0, 5, 0, 0, 0, 0, 'a'} // 声明5字节但只给1
	dec := NewDecoder(bytes.NewReader(append(good, trunc...)))
	var got Frame
	if err := dec.Next(&got); err != nil {
		t.Fatal(err)
	}
	if err := dec.Next(&got); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestEncodeRejectsBadInput(t *testing.T) {
	if _, err := Append(nil, &Frame{Version: 9, Payload: nil}); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("want ErrUnknownVersion, got %v", err)
	}
	big := make([]byte, MaxPayloadLen+1)
	if _, err := Append(nil, &Frame{Version: V1, Payload: big}); !errors.Is(err, ErrLengthExceeded) {
		t.Fatalf("want ErrLengthExceeded, got %v", err)
	}
}

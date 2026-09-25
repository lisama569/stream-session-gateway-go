package gateway

import (
	"testing"
)

func TestCodecSmoke(t *testing.T) {
	c := NewCodec(0, 0)
	d := NewDecoder(c)

	f1 := &Frame{Version: V1, Type: TypeData, Seq: 1, Payload: []byte("hello")}
	f2 := &Frame{Version: V2, Type: TypeData, Flags: 7, Seq: 2, Payload: []byte("world!!")}
	b1, _ := c.Encode(f1)
	b2, _ := c.Encode(f2)

	// 粘包：两帧 + 一个坏字节 + V1 半包
	stream := append([]byte{}, b1...)
	stream = append(stream, b2...)
	stream = append(stream, 0x77) // 垃圾字节
	stream = append(stream, b1[:5]...)

	frames, errs := d.Feed(stream)
	if len(frames) != 2 {
		t.Fatalf("want 2 frames, got %d errs=%v", len(frames), errs)
	}
	if string(frames[0].Payload) != "hello" || frames[1].Flags != 7 {
		t.Fatalf("frame content wrong: %+v %+v", frames[0], frames[1])
	}
	if len(errs) != 1 || errs[0].Kind != KindMalformedFrame {
		t.Fatalf("want 1 malformed err, got %v", errs)
	}
	if d.Buffered() != 5 {
		t.Fatalf("want 5 buffered half bytes, got %d", d.Buffered())
	}

	// 喂入剩余半包字节，应解出第 3 帧
	frames, errs = d.Feed(b1[5:])
	if len(frames) != 1 || len(errs) != 0 {
		t.Fatalf("want 1 frame 0 errs, got %d %v", len(frames), errs)
	}
	if d.Buffered() != 0 {
		t.Fatalf("want empty buffer, got %d", d.Buffered())
	}

	// 校验失败（翻转载荷字节）后能恢复到下一个好帧
	bad := append([]byte{}, b1...)
	bad[10] ^= 0xFF
	frames, errs = d.Feed(append(bad, b2...))
	if len(frames) != 1 || errs[0].Kind != KindChecksumMismatch {
		t.Fatalf("want checksum err then 1 frame, got frames=%d errs=%v", len(frames), errs)
	}

	// 未知版本
	frames, errs = d.Feed([]byte{Magic, 9})
	if len(frames) != 0 || errs[0].Kind != KindUnknownVersion {
		t.Fatalf("want unknown version, got %v", errs)
	}
}

package gateway

import (
	"testing"
)

func TestDecoderLengthExceededBoundedBuffer(t *testing.T) {
	t.Logf("场景: V2 声明载荷长度=1MiB+1（超过配置上限 1MiB），随流追加垃圾与好帧；" +
		"期望报 length_exceeded，解码器缓冲不随声明长度增长，好帧仍能解出")
	c := NewCodec(defaultMaxPayloadV1, defaultMaxPayloadV2)
	d := NewDecoder(c)

	// 构造声明长度超限的 V2 头部。
	hdr := []byte{Magic, byte(V2), byte(TypeData), 0, 0, 0x10, 0, 0x01, 0, 0, 0, 0}
	good, _ := c.Encode(&Frame{Version: V1, Type: TypeData, Seq: 9, Payload: []byte("z")})

	frames, errs := d.Feed(hdr)
	if len(frames) != 0 || len(errs) != 1 || errs[0].Kind != KindLengthExceeded {
		t.Fatalf("期望单个 length_exceeded, frames=%d errs=%v", len(frames), errs)
	}
	t.Logf("判定: 越界帧报 kind=%s msg=%s", errs[0].Kind, errs[0].Message)

	// 越界头之后应能重新同步到好帧，缓冲保持有界。
	frames, errs = d.Feed(good)
	if len(frames) != 1 || frames[0].Seq != 9 {
		t.Fatalf("期望好帧 seq=9, frames=%d errs=%v", len(frames), errs)
	}
	if d.Buffered() != 0 {
		t.Fatalf("缓冲应已清空, buffered=%d", d.Buffered())
	}
	t.Logf("判定: 缓冲 bounded（成帧后=0），未串包，好帧 seq=9 正常")
}

func TestDecoderV1LengthExceeded(t *testing.T) {
	t.Logf("场景: V1 声明长度超过 V1 上限，期望 length_exceeded（区别于未知版本/校验失败）")
	c := NewCodec(0, 0)
	d := NewDecoder(c)
	// V1 头部：magic ver type len=0xFFFF seq(4)
	hdr := []byte{Magic, byte(V1), byte(TypeData), 0xFF, 0xFF, 0, 0, 0, 0}
	_, errs := d.Feed(hdr)
	if len(errs) != 1 || errs[0].Kind != KindLengthExceeded {
		t.Fatalf("期望 length_exceeded, got %v", errs)
	}
	t.Logf("判定: V1 越界被区分为 %s", errs[0].Kind)
}

func TestDecoderChecksumUnknownVersionDistinguished(t *testing.T) {
	c := NewCodec(0, 0)
	d := NewDecoder(c)

	// 未知版本应区别于其它错误。
	_, errs := d.Feed([]byte{Magic, 77})
	if errs[0].Kind != KindUnknownVersion {
		t.Fatalf("期望 unknown_version, got %v", errs)
	}

	// V2 CRC 错误。
	bad, _ := c.Encode(&Frame{Version: V2, Type: TypeData, Payload: []byte("q")})
	bad[len(bad)-2] ^= 0x0F
	_, errs = d.Feed(bad)
	if errs[0].Kind != KindChecksumMismatch {
		t.Fatalf("期望 checksum_mismatch, got %v", errs)
	}
	t.Logf("场景: 未知版本/校验失败分别判定 unknown_version、checksum_mismatch，通过")
}

package frame

// xorChecksum 返回 payload 逐字节异或校验值（V1）。
// 空负载约定为 0。
func xorChecksum(p []byte) uint8 {
	var c uint8
	for _, b := range p {
		c ^= b
	}
	return c
}

// crc16CCITT 以 CRC-16/CCITT-FALSE 计算 payload（V2）。
// 多项式 0x1021，初始值 0xFFFF，不反转输入/输出，不异或输出。
func crc16CCITT(p []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range p {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

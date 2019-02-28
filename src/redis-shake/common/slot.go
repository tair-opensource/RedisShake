package utils

func KeyToSlot(key string) uint16 {
	hashtag := ""
	for i, s := range key {
		if s == '{' {
			for k := i; k < len(key); k++ {
				if key[k] == '}' {
					hashtag = key[i+1 : k]
					break
				}
			}
		}
	}
	if len(hashtag) > 0 {
		return crc16(hashtag) & 0x3fff
	}
	return crc16(key) & 0x3fff
}

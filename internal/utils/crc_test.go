package utils

import "testing"

func TestCrc16(t *testing.T) {
	ret := KeyHash("你")
	if ret != 8522 {
		t.Errorf("KeyHash failed, expect: %d, actual: %d", 8522, ret)
	}
}

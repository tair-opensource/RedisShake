package utils

import "testing"

func TestTargetServerTypeDiffers(t *testing.T) {
	defer targetServerType.Store(serverTypeUnknown)
	targetServerType.Store(serverTypeUnknown)
	if TargetServerTypeDiffers(true) || TargetServerTypeDiffers(false) {
		t.Fatal("an unknown target must not be reported as different")
	}
	SetTargetIsValkey(true)
	if !TargetServerTypeDiffers(false) || TargetServerTypeDiffers(true) {
		t.Fatal("Valkey target")
	}
	SetTargetIsValkey(false)
	if !TargetServerTypeDiffers(true) || TargetServerTypeDiffers(false) {
		t.Fatal("Redis target")
	}
}

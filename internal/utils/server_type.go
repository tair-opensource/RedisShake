package utils

import "sync/atomic"

const (
	serverTypeUnknown int32 = iota
	serverTypeRedis
	serverTypeValkey
)

var targetServerType atomic.Int32

// SetTargetIsValkey records the destination server type. Writers to Redis or
// Valkey call it once they are connected; other writers leave it unknown.
func SetTargetIsValkey(isValkey bool) {
	if isValkey {
		targetServerType.Store(serverTypeValkey)
	} else {
		targetServerType.Store(serverTypeRedis)
	}
}

// TargetServerTypeDiffers reports whether the destination is known to be a
// different server type (Redis or Valkey) than the source.
func TargetServerTypeDiffers(sourceIsValkey bool) bool {
	switch targetServerType.Load() {
	case serverTypeValkey:
		return !sourceIsValkey
	case serverTypeRedis:
		return sourceIsValkey
	}
	return false
}

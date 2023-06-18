package aof

import "github.com/alibaba/RedisShake/internal/entry"

// TODO: 待填充
type Loader struct {
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	return ld
}

func (ld *Loader) ParseRDB() int {
	// 加载aof目录
	// 进行check_aof， aof
	// 加载清单
	return 0
}

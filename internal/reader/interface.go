package reader

import "github.com/alibaba/RedisShake/internal/entry"

type Reader interface {
	StartRead() chan *entry.Entry
}

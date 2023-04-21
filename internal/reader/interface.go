package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/status"
)

type Reader interface {
	status.Statusable
	StartRead() chan *entry.Entry
}

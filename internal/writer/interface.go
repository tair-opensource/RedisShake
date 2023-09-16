package writer

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/status"
)

type Writer interface {
	status.Statusable
	Write(entry *entry.Entry)
	Close()
}

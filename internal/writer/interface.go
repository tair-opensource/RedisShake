package writer

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/status"
	"context"
)

type Writer interface {
	status.Statusable
	Write(entry *entry.Entry)
	StartWrite(ctx context.Context) (ch chan *entry.Entry)
	Close()
}

package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/status"
	"context"
)

type Reader interface {
	status.Statusable
	StartRead(ctx context.Context) []chan *entry.Entry
}

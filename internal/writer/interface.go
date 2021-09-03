package writer

import "github.com/alibaba/RedisShake/internal/entry"

type Writer interface {
	Write(entry *entry.Entry)
}

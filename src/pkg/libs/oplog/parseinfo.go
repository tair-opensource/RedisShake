package oplog

import (
	"bytes"
	"strings"
)

// ParseInfo convert result of info command to map[string]string.
// For example, "opapply_source_count:1\r\nopapply_source_0:server_id=3171317,applied_opid=1\r\n"
// is converted to map[string]string{"opapply_source_count": "1", "opapply_source_0": "server_id=3171317,applied_opid=1"}.
func ParseInfo(content []byte) map[string]string {
	result := make(map[string]string, 10)
	lines := bytes.Split(content, []byte("\r\n"))
	for i := 0; i < len(lines); i++ {
		items := bytes.SplitN(lines[i], []byte(":"), 2)
		if len(items) != 2 {
			continue
		}
		result[string(items[0])] = string(items[1])
	}
	return result
}

// ParseValue convert value of one item from info command result to map[string]string.
// For example, "server_id=3171317,applied_opid=1" is converted to map[string]string{"server_id": "3171317", "applied_opid": "1"}.
func ParseValue(content string) map[string]string {
	result := make(map[string]string, 2)
	items := strings.Split(content, ",")
	for i := 0; i < len(items); i++ {
		v := strings.SplitN(items[i], "=", 2)
		if len(v) == 2 {
			result[v[0]] = v[1]
		}
	}
	return result
}

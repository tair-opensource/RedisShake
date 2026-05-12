package aof

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"RedisShake/internal/entry"
)

// writeRESP encodes args as a RESP2 array (`*N\r\n$len\r\n<arg>\r\n...`).
func writeRESP(buf *bytes.Buffer, args ...string) {
	buf.WriteString("*")
	buf.WriteString(itoa(len(args)))
	buf.WriteString("\r\n")
	for _, a := range args {
		buf.WriteString("$")
		buf.WriteString(itoa(len(a)))
		buf.WriteString("\r\n")
		buf.WriteString(a)
		buf.WriteString("\r\n")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func writeAOF(t *testing.T, content []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.aof")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("write aof: %v", err)
	}
	return path
}

func loadAll(t *testing.T, path string) []*entry.Entry {
	t.Helper()
	ch := make(chan *entry.Entry, 1024)
	ld := NewLoader(path, ch)
	done := make(chan int, 1)
	go func() {
		done <- ld.LoadSingleAppendOnlyFile(context.Background(), 0)
		close(ch)
	}()
	var entries []*entry.Entry
	for e := range ch {
		entries = append(entries, e)
	}
	select {
	case ret := <-done:
		if ret != OK {
			t.Fatalf("LoadSingleAppendOnlyFile returned %d", ret)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("loader did not finish in time")
	}
	return entries
}

// TestLoadSingleAppendOnlyFile_BinaryArgWithCRLF reproduces issue #1046:
// a RESTORE-style command whose binary payload contains '\r\n' must round-trip
// correctly. Before the fix, the line-based reader truncated the value at the
// embedded CRLF and either panicked or desynced the stream.
func TestLoadSingleAppendOnlyFile_BinaryArgWithCRLF(t *testing.T) {
	binary := []byte{0x00, 0x05, 'a', '\r', '\n', 'b', 'c', 0xff, 0x0a, 0x0d, 0x42}
	var buf bytes.Buffer
	writeRESP(&buf, "RESTORE", "key", "0", string(binary))
	writeRESP(&buf, "SET", "next", "ok")

	entries := loadAll(t, writeAOF(t, buf.Bytes()))

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if got := entries[0].Argv; len(got) != 4 || got[0] != "RESTORE" || got[1] != "key" || got[2] != "0" || got[3] != string(binary) {
		t.Fatalf("RESTORE entry mismatch: %#v", got)
	}
	if got := entries[1].Argv; len(got) != 3 || got[0] != "SET" || got[1] != "next" || got[2] != "ok" {
		t.Fatalf("SET entry mismatch: %#v", got)
	}
}

func TestLoadSingleAppendOnlyFile_EmptyArg(t *testing.T) {
	var buf bytes.Buffer
	writeRESP(&buf, "SET", "key", "")

	entries := loadAll(t, writeAOF(t, buf.Bytes()))

	if len(entries) != 1 || len(entries[0].Argv) != 3 || entries[0].Argv[2] != "" {
		t.Fatalf("empty-value entry mismatch: %#v", entries)
	}
}

func TestLoadSingleAppendOnlyFile_PlainCommands(t *testing.T) {
	var buf bytes.Buffer
	writeRESP(&buf, "SET", "k1", "v1")
	writeRESP(&buf, "SET", "k2", "v2")

	entries := loadAll(t, writeAOF(t, buf.Bytes()))

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Argv[2] != "v1" || entries[1].Argv[2] != "v2" {
		t.Fatalf("entries mismatch: %#v", entries)
	}
}

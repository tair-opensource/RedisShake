package rotate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAOFReaderRotationThreshold(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pos    int64
		rotate bool
	}{
		{name: "below threshold", pos: MaxFileSize - 1},
		{name: "at threshold", pos: MaxFileSize},
		{name: "above threshold", pos: MaxFileSize + 1, rotate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			currentPath := filepath.Join(dir, "0.aof")
			file, err := os.Create(currentPath)
			if err != nil {
				t.Fatal(err)
			}
			timeout := 100 * time.Millisecond
			if tc.rotate {
				timeout = 5 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			// Model an EOF after consuming pos bytes without writing a 1 GiB file.
			// A nonzero starting offset ensures the guard uses pos, not offset.
			r := &AOFReader{
				ctx: ctx, name: "test", dir: dir, file: file,
				offset: 2*MaxFileSize + tc.pos, pos: tc.pos, filepath: currentPath,
			}
			defer r.Close()
			nextPath := filepath.Join(dir, fmt.Sprintf("%d.aof", r.offset))
			// A candidate file must be ignored until the rotation threshold is exceeded.
			if err := os.WriteFile(nextPath, []byte("x"), 0644); err != nil {
				t.Fatal(err)
			}

			buf := make([]byte, 1)
			n, err := r.Read(buf)
			if !tc.rotate {
				if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Read = (%d, %v), want (0, deadline exceeded)", n, err)
				}
				if r.filepath != currentPath {
					t.Fatalf("unexpected rotation to %s", r.filepath)
				}
				if _, err := os.Stat(currentPath); err != nil {
					t.Fatalf("current file must remain: %v", err)
				}
				return
			}
			if err != nil || n != 1 || buf[0] != 'x' {
				t.Fatalf("Read = (%d, %v, %q), want (1, nil, x)", n, err, buf[:n])
			}
			if r.filepath != nextPath || r.pos != 1 {
				t.Fatalf("unexpected next file state: path=%s pos=%d", r.filepath, r.pos)
			}
			if _, err := os.Stat(currentPath); !os.IsNotExist(err) {
				t.Fatalf("old file must be removed after rotation, got %v", err)
			}
		})
	}
}

type aofReadResult struct {
	data string
	err  error
}

func readAOFAsync(r *AOFReader, size int) <-chan aofReadResult {
	ch := make(chan aofReadResult, 1)
	go func() {
		buf := make([]byte, size)
		n, err := io.ReadFull(r, buf)
		ch <- aofReadResult{string(buf[:n]), err}
	}()
	return ch
}

func assertAOFReadPending(t *testing.T, ch <-chan aofReadResult) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("reader must still wait at tail: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
}

func assertAOFReadResult(t *testing.T, ch <-chan aofReadResult, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got.err != nil || got.data != want {
			t.Fatalf("got %+v, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not make progress")
	}
}

func TestAOFReaderTailAppendAndCancellation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const initialOffset int64 = 56789
	w := NewAOFWriter("review", dir, initialOffset)
	defer w.Close()
	r := NewAOFReader(ctx, "review", dir, initialOffset)
	defer r.Close()

	first := readAOFAsync(r, 3)
	assertAOFReadPending(t, first)
	w.Write([]byte("abc"))
	assertAOFReadResult(t, first, "abc")
	if r.Offset() != initialOffset+3 {
		t.Fatalf("bad offset %d", r.Offset())
	}

	second := readAOFAsync(r, 3)
	assertAOFReadPending(t, second)
	w.Write([]byte("def"))
	assertAOFReadResult(t, second, "def")
	if r.pos != 6 {
		t.Fatalf("bad pos %d", r.pos)
	}

	pending := readAOFAsync(r, 1)
	assertAOFReadPending(t, pending)
	cancel()
	select {
	case got := <-pending:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("bad cancellation: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not unblock reader")
	}
	if _, err := os.Stat(w.filepath); err != nil {
		t.Fatal("tail file unexpectedly deleted:", err)
	}
}

func TestAOFReaderWriterRotationBoundary(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const initialOffset int64 = 12345
	w := NewAOFWriter("review", dir, initialOffset)
	defer w.Close()
	oldPath := w.filepath
	// Sparse prefix avoids writing a GiB. Exercise real reads/writes around the boundary.
	prefix := int64(MaxFileSize - 1)
	if err := w.file.Truncate(prefix); err != nil {
		t.Fatal(err)
	}
	w.filesize = prefix
	w.offset = initialOffset + prefix
	r := NewAOFReader(ctx, "review", dir, initialOffset)
	defer r.Close()
	if _, err := r.file.Seek(prefix, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	r.pos, r.offset = prefix, initialOffset+prefix

	w.Write([]byte("a"))
	assertAOFReadResult(t, readAOFAsync(r, 1), "a")
	if r.pos != MaxFileSize || w.filepath != oldPath {
		t.Fatal("rotated at exact threshold")
	}
	pending := readAOFAsync(r, 3)
	assertAOFReadPending(t, pending)
	w.Write([]byte("b")) // exceeds threshold; real Sync, Close, then next file creation
	w.Write([]byte("cd"))
	assertAOFReadResult(t, pending, "bcd")
	if r.pos != 2 || r.Offset() != initialOffset+MaxFileSize+3 {
		t.Fatalf("bad post-rotation state: pos=%d offset=%d", r.pos, r.Offset())
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old segment not removed: %v", err)
	}
	w.Write([]byte("e"))
	assertAOFReadResult(t, readAOFAsync(r, 1), "e")
	if r.pos != 3 {
		t.Fatalf("new segment pos did not advance: %d", r.pos)
	}
}

func TestAOFReaderWaitsForNextSegment(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "0.aof")
	file, err := os.Create(old)
	if err != nil {
		t.Fatal(err)
	}
	size := int64(MaxFileSize + 7)
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := NewAOFReader(ctx, "review", dir, 0)
	defer r.Close()
	if _, err := r.file.Seek(size, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	r.pos, r.offset = size, size
	pending := readAOFAsync(r, 1)
	assertAOFReadPending(t, pending)
	next := filepath.Join(dir, fmt.Sprintf("%d.aof", size))
	if err := os.WriteFile(next, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	assertAOFReadResult(t, pending, "x")
	if r.pos != 1 || r.offset != size+1 {
		t.Fatalf("bad state: pos=%d offset=%d", r.pos, r.offset)
	}
}

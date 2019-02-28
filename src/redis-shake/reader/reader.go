package reader

import (
	"bufio"
	"fmt"
)

type ErrorReply string

var (
	IgnoreReply string = "ignore"
)

type ReplyReader struct {
	reader *bufio.Reader
}

func NewReplyReader(r *bufio.Reader) *ReplyReader {
	return &ReplyReader{r}
}

func (r *ReplyReader) readLine() ([]byte, error) {
	p, err := r.reader.ReadSlice('\n')

	if err == bufio.ErrBufferFull {
		return nil, fmt.Errorf("ReaderBufferFull")
	}

	if err != nil {
		return nil, fmt.Errorf("ReadSliceFail:%s", err.Error())
	}

	pos := len(p) - 2
	if pos < 0 || p[pos] != '\r' {
		return nil, fmt.Errorf("BadRedisReply")
	}

	return p[:pos], nil
}

func (r *ReplyReader) ReadNextReply() (interface{}, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}

		if len(line) == 0 {
			return nil, fmt.Errorf("EmptyReply")
		}

		switch line[0] {
		case '-':
			return ErrorReply(line[1:]), nil
		case '+', ':', '$', '*':
			return IgnoreReply, nil
		}
		continue
	}
	return "", fmt.Errorf("BUG")
}

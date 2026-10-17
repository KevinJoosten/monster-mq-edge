package redisserver

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type protoError struct {
	msg string
}

func (e *protoError) Error() string { return e.msg }

func errProtocol(format string, args ...any) *protoError {
	return &protoError{msg: fmt.Sprintf(format, args...)}
}

// readCommand reads a single RESP command (multibulk or inline) from r.
// maxMsgSize bounds bulk length (if <= 0, 512 MiB is used).
func readCommand(r *bufio.Reader, maxMsgSize int) ([][]byte, error) {
	leadByte, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	if leadByte[0] == '*' {
		return readMultibulk(r, maxMsgSize)
	}
	return readInline(r)
}

func readMultibulk(r *bufio.Reader, maxMsgSize int) ([][]byte, error) {
	line, err := readLine(r, 65536)
	if err != nil {
		return nil, err
	}
	if len(line) < 1 || line[0] != '*' {
		return nil, errProtocol("ERR Protocol error: invalid multibulk length")
	}
	count, err := strconv.Atoi(string(line[1:]))
	if err != nil || count < 0 || count > 1048576 {
		return nil, errProtocol("ERR Protocol error: invalid multibulk length")
	}

	effectiveMaxBulk := maxMsgSize
	if effectiveMaxBulk <= 0 {
		effectiveMaxBulk = 512 * 1024 * 1024 // 512 MiB
	}

	args := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		firstByte, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if firstByte != '$' {
			return nil, errProtocol("ERR Protocol error: expected '$', got '%c'", firstByte)
		}

		lenLine, err := readLine(r, 65536)
		if err != nil {
			return nil, err
		}
		bulkLen, err := strconv.Atoi(string(lenLine))
		if err != nil || bulkLen < 0 || bulkLen > effectiveMaxBulk {
			return nil, errProtocol("ERR Protocol error: invalid bulk length")
		}

		data := make([]byte, bulkLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}

		crlf := make([]byte, 2)
		if _, err := io.ReadFull(r, crlf); err != nil {
			return nil, err
		}
		if crlf[0] != '\r' || crlf[1] != '\n' {
			return nil, errProtocol("ERR Protocol error: invalid bulk length")
		}

		args = append(args, data)
	}

	return args, nil
}

func readInline(r *bufio.Reader) ([][]byte, error) {
	line, err := readLine(r, 65536)
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(line))
	if len(fields) == 0 {
		return [][]byte{}, nil
	}
	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = []byte(f)
	}
	return args, nil
}

func readLine(r *bufio.Reader, maxLen int) ([]byte, error) {
	var buf []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\r' {
			next, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			if next == '\n' {
				return buf, nil
			}
			buf = append(buf, b, next)
		} else {
			buf = append(buf, b)
		}
		if len(buf) > maxLen {
			return nil, errProtocol("ERR Protocol error: too big inline request")
		}
	}
}

type respValue interface {
	writeResp(w *bufio.Writer) error
}

type respSimple string

func (v respSimple) writeResp(w *bufio.Writer) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", string(v))
	return err
}

type respErr string

func (v respErr) writeResp(w *bufio.Writer) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", string(v))
	return err
}

type respInteger int64

func (v respInteger) writeResp(w *bufio.Writer) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", int64(v))
	return err
}

type respBulk []byte

func (v respBulk) writeResp(w *bufio.Writer) error {
	if v == nil {
		_, err := w.WriteString("$-1\r\n")
		return err
	}
	if _, err := fmt.Fprintf(w, "$%d\r\n", len(v)); err != nil {
		return err
	}
	if _, err := w.Write(v); err != nil {
		return err
	}
	_, err := w.WriteString("\r\n")
	return err
}

type respArray []respValue

func (v respArray) writeResp(w *bufio.Writer) error {
	if v == nil {
		_, err := w.WriteString("*-1\r\n")
		return err
	}
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(v)); err != nil {
		return err
	}
	for _, item := range v {
		if err := item.writeResp(w); err != nil {
			return err
		}
	}
	return nil
}

func respBulkStr(s string) respValue {
	return respBulk([]byte(s))
}

func respNullBulk() respValue {
	return respBulk(nil)
}

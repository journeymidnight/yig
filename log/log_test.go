package log_test

import (
	"bytes"
	"github.com/journeymidnight/yig/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

type closeBuffer struct {
	*bytes.Buffer
}

func (b closeBuffer) Close() error {
	b.Buffer.Reset()
	return nil
}

func TestLogger(t *testing.T) {
	buf := closeBuffer{
		Buffer: &bytes.Buffer{},
	}
	l := log.NewLogger(buf, log.InfoLevel)

	l.Info("aaaaa")
	l.Warn("bbbbb")
	l.Error("ccccc")
	l.Println("hehe")
	s := buf.String()
	assert.Contains(t, s, "[INFO]")
	assert.Contains(t, s, "[WARN]")
	assert.Contains(t, s, "[ERROR]")
	assert.Contains(t, s, "aaaaa")
	assert.Contains(t, s, "bbbbb")
	assert.Contains(t, s, "ccccc")
	assert.Contains(t, s, "hehe")
	// request ID
	ll := l.NewWithRequestID("request-id")
	ll.Info("haha")
	s = buf.String()
	assert.Contains(t, s, "haha")
	assert.Contains(t, s, "request-id")
}

func TestLogLevel(t *testing.T) {
	errBuf := closeBuffer{
		Buffer: &bytes.Buffer{},
	}
	errLogger := log.NewLogger(errBuf, log.ErrorLevel)
	errLogger.Info("aaa")
	errLogger.Warn("bbb")
	errLogger.Error("ccc")
	errString := errBuf.String()
	assert.NotContains(t, errString, "[INFO]")
	assert.NotContains(t, errString, "aaa")
	assert.NotContains(t, errString, "[WARN]")
	assert.NotContains(t, errString, "bbb")
	assert.Contains(t, errString, "[ERROR]")
	assert.Contains(t, errString, "ccc")

	warnBuf := closeBuffer{
		Buffer: &bytes.Buffer{},
	}
	warnLogger := log.NewLogger(warnBuf, log.WarnLevel)
	warnLogger.Info("aaa")
	warnLogger.Warn("bbb")
	warnLogger.Error("ccc")
	warnString := warnBuf.String()
	assert.NotContains(t, warnString, "[INFO]")
	assert.NotContains(t, warnString, "aaa")
	assert.Contains(t, warnString, "[WARN]")
	assert.Contains(t, warnString, "bbb")
	assert.Contains(t, warnString, "[ERROR]")
	assert.Contains(t, warnString, "ccc")
}

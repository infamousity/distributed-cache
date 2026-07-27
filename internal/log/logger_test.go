package log

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestErrorfFormatsMessage(t *testing.T) {
	var out bytes.Buffer
	logger := Logger(WithSink(&out), WithLevel(LevelError))

	logger.Errorf("operation failed: %v", errors.New("boom"))

	got := out.String()
	if !strings.Contains(got, `"msg":"operation failed: boom"`) {
		t.Fatalf("Errorf output = %q, want formatted message", got)
	}
	if strings.Contains(got, "%v") || strings.Contains(got, "!BADKEY") {
		t.Fatalf("Errorf output contains malformed structured arguments: %q", got)
	}
}

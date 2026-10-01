package logging

import (
	"bytes"
	"strings"
	"testing"
)

func logAll(t *testing.T, level string) string {
	t.Helper()
	var buf bytes.Buffer
	log, err := New(&buf, level)
	if err != nil {
		t.Fatalf("New(%q) error = %v", level, err)
	}
	log.Debug("ligne-debug")
	log.Info("ligne-info")
	log.Warn("ligne-warn")
	return buf.String()
}

func TestNewFiltersByLevel(t *testing.T) {
	tests := []struct {
		level                         string
		wantDebug, wantInfo, wantWarn bool
	}{
		{"", false, true, true}, // défaut : info
		{"info", false, true, true},
		{"INFO", false, true, true},
		{"debug", true, true, true},
		{"warn", false, false, true},
	}
	for _, tt := range tests {
		t.Run("niveau "+tt.level, func(t *testing.T) {
			out := logAll(t, tt.level)
			for name, want := range map[string]bool{"ligne-debug": tt.wantDebug, "ligne-info": tt.wantInfo, "ligne-warn": tt.wantWarn} {
				if got := strings.Contains(out, name); got != want {
					t.Errorf("%s présent = %v, want %v (sortie : %s)", name, got, want, out)
				}
			}
		})
	}
}

func TestNewRejectsAnUnknownLevel(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "bavard"); err == nil {
		t.Error("New(\"bavard\") a réussi, want une erreur")
	}
}

func TestNewWritesJSON(t *testing.T) {
	if out := logAll(t, "info"); !strings.HasPrefix(out, "{") {
		t.Errorf("la sortie n'est pas du JSON : %s", out)
	}
}

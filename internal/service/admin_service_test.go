package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

func TestAdminCallbackToggleDryRun(t *testing.T) {
	t.Parallel()

	editCalls := 0
	answerCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/editMessageText":
			editCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{}})
		case "/answerCallbackQuery":
			answerCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	keywordPath := filepath.Join(tmpDir, "keywords.json")
	if err := os.WriteFile(keywordPath, []byte("{\"keywords\":[\"a\"]}"), 0o644); err != nil {
		t.Fatalf("write keywords: %v", err)
	}

	keywordStore := storage.NewKeywordStore(keywordPath)
	if _, err := keywordStore.Load(); err != nil {
		t.Fatalf("load keywords: %v", err)
	}

	settingsStore, err := storage.NewSettingsStore(filepath.Join(tmpDir, "settings.json"), storage.RuntimeSettings{
		MonitoringEnabled: true,
		CooldownMinutes:   60,
		DMTemplate:        "hi",
		DryRun:            false,
	})
	if err != nil {
		t.Fatalf("new settings: %v", err)
	}

	admin := NewAdminService(123, tg.NewClientWithBaseURL(server.URL), keywordStore, settingsStore, logx.New("debug"))
	handled, err := admin.HandleUpdate(context.Background(), model.Update{
		UpdateID: 1,
		CallbackQuery: &model.CallbackQuery{
			ID:   "cb1",
			From: &model.User{ID: 123},
			Data: callbackToggleDryRun,
			Message: &model.Message{
				MessageID: 5,
				Chat:      model.Chat{ID: 999},
			},
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	if !handled {
		t.Fatalf("expected admin callback to be handled")
	}
	if !settingsStore.IsDryRun() {
		t.Fatalf("expected dry-run to be enabled")
	}
	if editCalls != 1 || answerCalls != 1 {
		t.Fatalf("expected 1 edit and 1 answer call, got edit=%d answer=%d", editCalls, answerCalls)
	}
}

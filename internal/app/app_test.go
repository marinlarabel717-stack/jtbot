package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

func TestSendDMDoesNotFallbackToMonitorAccount(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendMessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		sendCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 1,
			},
		})
	}))
	defer server.Close()

	app := newTestApp(t)
	fallback := tg.NewClientWithBaseURL(server.URL)

	senderLabel, err := app.SendDM(context.Background(), fallback, model.DMJob{
		TargetUserID: 12345,
		Username:     "target_user",
	}, model.ParseDMTemplate(model.EncodeTextDMTemplate("hello")))
	if err == nil {
		t.Fatal("expected send dm to fail without dm accounts")
	}
	if senderLabel != "" {
		t.Fatalf("expected empty sender label, got %q", senderLabel)
	}
	if got := sendCalls.Load(); got != 0 {
		t.Fatalf("expected fallback monitor account to stay unused, got %d send calls", got)
	}
}

func TestSendDMUsesUploadedDMAccount(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendMessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		sendCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 1,
			},
		})
	}))
	defer server.Close()

	app := newTestApp(t)
	account, added, err := app.dmStore.Add("+15550001")
	if err != nil {
		t.Fatalf("add dm account: %v", err)
	}
	if !added {
		t.Fatal("expected dm account to be added")
	}
	app.dmAccounts[account.Phone] = &monitorRuntime{
		online: true,
		client: tg.NewClientWithBaseURL(server.URL),
	}

	senderLabel, err := app.SendDM(context.Background(), nil, model.DMJob{
		TargetUserID: 67890,
		Username:     "real_dm",
	}, model.ParseDMTemplate(model.EncodeTextDMTemplate("hello")))
	if err != nil {
		t.Fatalf("expected dm send to succeed, got %v", err)
	}
	if senderLabel != account.Phone {
		t.Fatalf("expected sender label %q, got %q", account.Phone, senderLabel)
	}
	if got := sendCalls.Load(); got != 1 {
		t.Fatalf("expected 1 dm send call, got %d", got)
	}
}

func newTestApp(t *testing.T) *App {
	t.Helper()

	tmpDir := t.TempDir()
	dmStore, err := storage.NewDMAccountStore(filepath.Join(tmpDir, "dm_accounts.json"), filepath.Join(tmpDir, "dm_sessions"))
	if err != nil {
		t.Fatalf("new dm store: %v", err)
	}

	return &App{
		logger:     logx.New("debug"),
		dmStore:    dmStore,
		dmAccounts: make(map[string]*monitorRuntime),
	}
}

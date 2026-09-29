package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

func TestAdminCallbackToggleDryRun(t *testing.T) {
	t.Parallel()

	client, calls := newAdminTestClient(t)
	admin, settingsStore, _ := newAdminTestService(t, client)

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
	if calls.edit != 1 || calls.answer != 1 {
		t.Fatalf("expected 1 edit and 1 answer call, got edit=%d answer=%d", calls.edit, calls.answer)
	}
}

func TestAdminCallbackToggleFilterNoUsername(t *testing.T) {
	t.Parallel()

	client, _ := newAdminTestClient(t)
	admin, settingsStore, _ := newAdminTestService(t, client)

	handled, err := admin.HandleUpdate(context.Background(), model.Update{
		UpdateID: 1,
		CallbackQuery: &model.CallbackQuery{
			ID:   "cb2",
			From: &model.User{ID: 123},
			Data: callbackToggleNoName,
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
	if !settingsStore.FilterNoUsername() {
		t.Fatalf("expected username filter to be enabled")
	}
}

func TestAdminCallbackBlockUser(t *testing.T) {
	t.Parallel()

	client, calls := newAdminTestClient(t)
	admin, _, blacklistStore := newAdminTestService(t, client)

	handled, err := admin.HandleUpdate(context.Background(), model.Update{
		UpdateID: 1,
		CallbackQuery: &model.CallbackQuery{
			ID:   "cb3",
			From: &model.User{ID: 123},
			Data: callbackBlockUser + "777:target_user",
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
	users := blacklistStore.Users()
	if len(users) != 1 || users[0].UserID != 777 {
		t.Fatalf("expected user 777 to be blacklisted, got %+v", users)
	}
	if calls.edit != 0 {
		t.Fatalf("expected no edit call for blacklist callback, got %d", calls.edit)
	}
	if calls.answer != 1 {
		t.Fatalf("expected 1 answer call, got %d", calls.answer)
	}
}

func TestAdminPendingUnblockUser(t *testing.T) {
	t.Parallel()

	client, calls := newAdminTestClient(t)
	admin, _, blacklistStore := newAdminTestService(t, client)
	added, err := blacklistStore.AddUser(777, "target_user")
	if err != nil {
		t.Fatalf("add user: %v", err)
	}
	if !added {
		t.Fatalf("expected user to be added to blacklist")
	}
	admin.setPending(123, pendingUnblockUsers)

	handled, err := admin.HandleUpdate(context.Background(), model.Update{
		UpdateID: 2,
		Message: &model.Message{
			MessageID: 6,
			From:      &model.User{ID: 123},
			Chat:      model.Chat{ID: 999},
			Text:      "777",
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	if !handled {
		t.Fatalf("expected admin message to be handled")
	}
	if len(blacklistStore.Users()) != 0 {
		t.Fatalf("expected blacklist to be empty after unblock")
	}
	if calls.send == 0 {
		t.Fatalf("expected a sendMessage call after unblock")
	}
}

func TestAdminMessageSubmitsPendingLoginCode(t *testing.T) {
	t.Parallel()

	client, calls := newAdminTestClient(t)
	authInput := tg.NewAdminAuth("+123456", 123, client, logx.New("debug"))
	admin, _, _ := newAdminTestServiceWithAuth(t, client, authInput)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		code, err := authInput.Code(ctx, nil)
		if err != nil {
			errCh <- err
			return
		}
		codeCh <- code
	}()

	deadline := time.Now().Add(2 * time.Second)
	for calls.send == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.send == 0 {
		t.Fatal("expected login prompt to be sent before admin reply")
	}

	handled, err := admin.HandleUpdate(context.Background(), model.Update{
		UpdateID: 3,
		Message: &model.Message{
			MessageID: 7,
			From:      &model.User{ID: 123},
			Chat:      model.Chat{ID: 999},
			Text:      "54321",
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	if !handled {
		t.Fatalf("expected admin login input to be handled")
	}

	select {
	case err := <-errCh:
		t.Fatalf("auth prompt returned error: %v", err)
	case code := <-codeCh:
		if code != "54321" {
			t.Fatalf("expected code 54321, got %q", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auth code")
	}

	if calls.send < 2 {
		t.Fatalf("expected prompt and ack sendMessage calls, got %d", calls.send)
	}
}

type adminTestCalls struct {
	send   int
	edit   int
	answer int
}

func newAdminTestClient(t *testing.T) (*tg.Client, *adminTestCalls) {
	t.Helper()

	calls := &adminTestCalls{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sendMessage":
			calls.send++
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
		case "/editMessageText":
			calls.edit++
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{}})
		case "/answerCallbackQuery":
			calls.answer++
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	return tg.NewClientWithBaseURL(server.URL), calls
}

func newAdminTestService(t *testing.T, client *tg.Client) (*AdminService, *storage.SettingsStore, *storage.BlacklistStore) {
	return newAdminTestServiceWithAuth(t, client, nil)
}

func newAdminTestServiceWithAuth(t *testing.T, client *tg.Client, authInput *tg.AdminAuth) (*AdminService, *storage.SettingsStore, *storage.BlacklistStore) {
	t.Helper()

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

	blacklistStore, err := storage.NewBlacklistStore(filepath.Join(tmpDir, "blacklist.json"))
	if err != nil {
		t.Fatalf("new blacklist: %v", err)
	}

	admin := NewAdminService(123, client, keywordStore, settingsStore, blacklistStore, authInput, logx.New("debug"))
	return admin, settingsStore, blacklistStore
}

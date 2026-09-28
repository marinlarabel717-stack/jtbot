package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/matcher"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/queue"
	"github.com/marinlarabel717-stack/jtbot/internal/rules"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

func TestTriggerServiceSkipsBlacklistedUser(t *testing.T) {
	t.Parallel()

	svc, recordStore, blacklistStore := newTriggerTestService(t, storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    []int64{-100123},
		CooldownMinutes:   60,
		DMTemplate:        "hi",
	})

	added, err := blacklistStore.AddUser(777, "target_user")
	if err != nil {
		t.Fatalf("add user: %v", err)
	}
	if !added {
		t.Fatalf("expected blacklist add to succeed")
	}

	err = svc.HandleUpdate(context.Background(), model.Update{
		UpdateID: 1,
		Message: &model.Message{
			MessageID: 99,
			From: &model.User{
				ID:       777,
				Username: "target_user",
			},
			Chat: model.Chat{
				ID:    -100123,
				Title: "test-group",
			},
			Text: "我想合作",
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	if got := len(recordStore.MatchRecords()); got != 0 {
		t.Fatalf("expected 0 match records, got %d", got)
	}
}

func TestTriggerServiceSkipsNoUsernameWhenEnabled(t *testing.T) {
	t.Parallel()

	svc, recordStore, _ := newTriggerTestService(t, storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    []int64{-100123},
		CooldownMinutes:   60,
		FilterNoUsername:  true,
		DMTemplate:        "hi",
	})

	err := svc.HandleUpdate(context.Background(), model.Update{
		UpdateID: 1,
		Message: &model.Message{
			MessageID: 99,
			From: &model.User{
				ID: 777,
			},
			Chat: model.Chat{
				ID:    -100123,
				Title: "test-group",
			},
			Text: "我想合作",
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	if got := len(recordStore.MatchRecords()); got != 0 {
		t.Fatalf("expected 0 match records, got %d", got)
	}
}

func TestTriggerServiceSkipsLongMessageWhenEnabled(t *testing.T) {
	t.Parallel()

	svc, recordStore, _ := newTriggerTestService(t, storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    []int64{-100123},
		CooldownMinutes:   60,
		MaxMessageLength:  3,
		DMTemplate:        "hi",
	})

	err := svc.HandleUpdate(context.Background(), model.Update{
		UpdateID: 1,
		Message: &model.Message{
			MessageID: 99,
			From: &model.User{
				ID:       777,
				Username: "target_user",
			},
			Chat: model.Chat{
				ID:    -100123,
				Title: "test-group",
			},
			Text: "我想合作",
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}
	if got := len(recordStore.MatchRecords()); got != 0 {
		t.Fatalf("expected 0 match records, got %d", got)
	}
}

func newTriggerTestService(t *testing.T, defaults storage.RuntimeSettings) (*TriggerService, *storage.RecordStore, *storage.BlacklistStore) {
	t.Helper()

	tmpDir := t.TempDir()
	recordStore, err := storage.NewRecordStore(filepath.Join(tmpDir, "records.json"))
	if err != nil {
		t.Fatalf("new record store: %v", err)
	}

	keywordPath := filepath.Join(tmpDir, "keywords.json")
	if err := os.WriteFile(keywordPath, []byte("{\"keywords\":[\"合作\"]}"), 0o644); err != nil {
		t.Fatalf("write keywords file: %v", err)
	}
	keywordStore := storage.NewKeywordStore(keywordPath)
	if _, err := keywordStore.Load(); err != nil {
		t.Fatalf("load keywords: %v", err)
	}

	settingsStore, err := storage.NewSettingsStore(filepath.Join(tmpDir, "settings.json"), defaults)
	if err != nil {
		t.Fatalf("new settings store: %v", err)
	}

	blacklistStore, err := storage.NewBlacklistStore(filepath.Join(tmpDir, "blacklist.json"))
	if err != nil {
		t.Fatalf("new blacklist store: %v", err)
	}

	client := tg.NewClientWithBaseURL("http://127.0.0.1")
	jobQueue := queue.NewMessageQueue(8)
	m := matcher.NewKeywordMatcher()
	ruleEngine := rules.NewEngine(settingsStore, recordStore)

	return NewTriggerService(m, keywordStore, ruleEngine, jobQueue, recordStore, client, nil, settingsStore, blacklistStore, logx.New("debug")), recordStore, blacklistStore
}

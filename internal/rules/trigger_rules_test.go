package rules

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
)

func TestCanQueueDMBlocksUserChatAndTextCooldowns(t *testing.T) {
	t.Parallel()

	engine, recordStore := newRuleTestEngine(t, storage.RuntimeSettings{
		MonitoringEnabled:   true,
		CooldownMinutes:     60,
		ChatCooldownMinutes: 120,
		TextCooldownMinutes: 180,
		DMTemplate:          "hi",
	})

	recordStore.SaveDMRecord(model.DMRecord{
		UserID:     1001,
		ChatID:     -1001,
		SourceText: "hello world",
		Status:     "sent",
		SentAt:     time.Now(),
	})

	if engine.CanQueueDM(model.DMJob{TargetUserID: 1001, ChatID: -1002, SourceText: "other"}) {
		t.Fatal("expected user cooldown to block repeat dm")
	}
	if reason := engine.ExplainDMBlockReason(model.DMJob{TargetUserID: 1001, ChatID: -1002, SourceText: "other"}); reason == "" {
		t.Fatal("expected plain-language block reason for user cooldown")
	}

	engineNoUserCooldown, recordStoreNoUser := newRuleTestEngine(t, storage.RuntimeSettings{
		MonitoringEnabled:   true,
		CooldownMinutes:     0,
		ChatCooldownMinutes: 120,
		TextCooldownMinutes: 180,
		DMTemplate:          "hi",
	})
	recordStoreNoUser.SaveDMRecord(model.DMRecord{
		UserID:     1001,
		ChatID:     -1001,
		SourceText: "hello world",
		Status:     "sent",
		SentAt:     time.Now(),
	})

	if engineNoUserCooldown.CanQueueDM(model.DMJob{TargetUserID: 1001, ChatID: -1001, SourceText: "other"}) {
		t.Fatal("expected same chat cooldown to block repeat dm")
	}
	if reason := engineNoUserCooldown.ExplainDMBlockReason(model.DMJob{TargetUserID: 1001, ChatID: -1001, SourceText: "other"}); reason == "" {
		t.Fatal("expected plain-language block reason for chat cooldown")
	}
	if engineNoUserCooldown.CanQueueDM(model.DMJob{TargetUserID: 1001, ChatID: -1002, SourceText: "hello world"}) {
		t.Fatal("expected same text cooldown to block repeat dm")
	}
	if reason := engineNoUserCooldown.ExplainDMBlockReason(model.DMJob{TargetUserID: 1001, ChatID: -1002, SourceText: "hello world"}); reason == "" {
		t.Fatal("expected plain-language block reason for text cooldown")
	}
	if !engineNoUserCooldown.CanQueueDM(model.DMJob{TargetUserID: 1001, ChatID: -1002, SourceText: "fresh text"}) {
		t.Fatal("expected fresh chat/text combination to pass")
	}
}

func newRuleTestEngine(t *testing.T, defaults storage.RuntimeSettings) (*Engine, *storage.RecordStore) {
	t.Helper()

	tmpDir := t.TempDir()
	recordStore, err := storage.NewRecordStore(filepath.Join(tmpDir, "records.json"))
	if err != nil {
		t.Fatalf("new record store: %v", err)
	}
	settingsStore, err := storage.NewSettingsStore(filepath.Join(tmpDir, "settings.json"), defaults)
	if err != nil {
		t.Fatalf("new settings store: %v", err)
	}
	return NewEngine(settingsStore, recordStore), recordStore
}

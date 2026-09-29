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
	"github.com/marinlarabel717-stack/jtbot/internal/matcher"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/queue"
	"github.com/marinlarabel717-stack/jtbot/internal/rules"
	"github.com/marinlarabel717-stack/jtbot/internal/sender"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

func TestPipelineQueuesAndSendsDM(t *testing.T) {
	t.Parallel()

	sendCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendMessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		sendCalls++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 1,
			},
		})
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	recordStore, err := storage.NewRecordStore(filepath.Join(tmpDir, "records.json"))
	if err != nil {
		t.Fatalf("new record store: %v", err)
	}

	keywordPath := filepath.Join(tmpDir, "keywords.json")
	if err := os.WriteFile(keywordPath, []byte("{\"keywords\":[\"合作\",\"私信\"]}"), 0o644); err != nil {
		t.Fatalf("write keywords file: %v", err)
	}
	keywordStore := storage.NewKeywordStore(keywordPath)
	if _, err := keywordStore.Load(); err != nil {
		t.Fatalf("load keywords: %v", err)
	}

	settingsStore, err := storage.NewSettingsStore(filepath.Join(tmpDir, "settings.json"), storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    []int64{-100123},
		CooldownMinutes:   60,
		DMTemplate:        "你好，看到你提到 {keywords}",
		DryRun:            false,
	})
	if err != nil {
		t.Fatalf("new settings store: %v", err)
	}

	logger := logx.New("debug")
	client := tg.NewClientWithBaseURL(server.URL)
	jobQueue := queue.NewMessageQueue(8)
	m := matcher.NewKeywordMatcher()
	ruleEngine := rules.NewEngine(settingsStore, recordStore)
	dmSender := sender.New(client, recordStore, settingsStore, logger)
	svc := NewTriggerService(m, keywordStore, ruleEngine, jobQueue, recordStore, client, nil, settingsStore, nil, "test-monitor", logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dmSender.Run(ctx, jobQueue.Consume())

	err = svc.HandleUpdate(ctx, model.Update{
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
			Text: "我想合作，欢迎私信我",
		},
	})
	if err != nil {
		t.Fatalf("handle update: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(recordStore.DMRecords()) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	matchRecords := recordStore.MatchRecords()
	if len(matchRecords) != 2 {
		t.Fatalf("expected 2 match records, got %d", len(matchRecords))
	}

	dmRecords := recordStore.DMRecords()
	if len(dmRecords) != 1 {
		t.Fatalf("expected 1 dm record, got %d", len(dmRecords))
	}
	if dmRecords[0].Status != "sent" {
		t.Fatalf("expected dm status sent, got %s", dmRecords[0].Status)
	}
	if sendCalls != 1 {
		t.Fatalf("expected 1 sendMessage call, got %d", sendCalls)
	}
}

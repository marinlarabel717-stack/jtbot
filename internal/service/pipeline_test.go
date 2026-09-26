package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	recordStore, err := storage.NewRecordStore(filepath.Join(t.TempDir(), "records.json"))
	if err != nil {
		t.Fatalf("new record store: %v", err)
	}

	logger := logx.New("debug")
	client := tg.NewClientWithBaseURL(server.URL)
	jobQueue := queue.NewMessageQueue(8)
	m := matcher.NewKeywordMatcher([]string{"合作", "私信"})
	ruleEngine := rules.NewEngine(map[int64]struct{}{-100123: {}}, time.Hour, recordStore)
	dmSender := sender.New(client, recordStore, "你好，看到你提到 {keywords}", false, logger)
	svc := NewTriggerService(m, ruleEngine, jobQueue, recordStore, client, 0, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dmSender.Run(ctx, jobQueue.Consume())

	err = svc.HandleUpdate(ctx, model.Update{
		UpdateID: 1,
		Message: model.Message{
			MessageID: 99,
			From: &model.User{
				ID:       777,
				Username: "target_user",
			},
			Chat: model.Chat{
				ID:    -100123,
				Title: "测试群",
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

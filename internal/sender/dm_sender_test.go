package sender

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type stubDispatcher struct {
	label string
	err   error
}

func (s stubDispatcher) SendDM(context.Context, *tg.Client, model.DMJob, string) (string, error) {
	return s.label, s.err
}

func TestSenderNotifiesSuccess(t *testing.T) {
	t.Parallel()

	messages := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		text, _ := payload["text"].(string)
		messages = append(messages, text)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer server.Close()

	recordStore, settingsStore := newSenderTestStores(t, 999)
	s := New(nil, stubDispatcher{label: "+15550001"}, recordStore, settingsStore, logx.New("debug"), tg.NewClientWithBaseURL(server.URL))

	s.handleJob(context.Background(), model.DMJob{
		TargetUserID: 2019667492,
		TargetLabel:  "大户人家-数据 (@UUZVI)",
		Username:     "UUZVI",
		ChatTitle:    "头铁出海 项目资源交流 4群",
		ChatLink:     "https://t.me/toutiechuhai04",
		Keywords:     []string{"TG"},
	})

	if len(messages) != 1 {
		t.Fatalf("expected 1 success notification, got %d", len(messages))
	}
	if !strings.Contains(messages[0], "私信发送成功") {
		t.Fatalf("expected success notification, got %q", messages[0])
	}
	if !strings.Contains(messages[0], "目标用户: 大户人家-数据 (@UUZVI) (2019667492)") {
		t.Fatalf("expected target user in notification, got %q", messages[0])
	}
	if !strings.Contains(messages[0], "群组链接: https://t.me/toutiechuhai04") {
		t.Fatalf("expected chat link in notification, got %q", messages[0])
	}
}

func TestSenderNotifiesFailureInChinese(t *testing.T) {
	t.Parallel()

	messages := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		text, _ := payload["text"].(string)
		messages = append(messages, text)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer server.Close()

	recordStore, settingsStore := newSenderTestStores(t, 999)
	s := New(nil, stubDispatcher{label: "+15550002", err: context.DeadlineExceeded}, recordStore, settingsStore, logx.New("debug"), tg.NewClientWithBaseURL(server.URL))

	s.handleJob(context.Background(), model.DMJob{
		TargetUserID: 123456,
		Username:     "failed_user",
		ChatTitle:    "测试群",
		ChatLink:     "https://t.me/test_group",
		Keywords:     []string{"TG"},
	})

	if len(messages) != 1 {
		t.Fatalf("expected 1 failure notification, got %d", len(messages))
	}
	if !strings.Contains(messages[0], "私信发送失败") {
		t.Fatalf("expected failure notification, got %q", messages[0])
	}
	if !strings.Contains(messages[0], "失败原因") {
		t.Fatalf("expected failure reason in notification, got %q", messages[0])
	}
}

func newSenderTestStores(t *testing.T, alertChatID int64) (*storage.RecordStore, *storage.SettingsStore) {
	t.Helper()

	tmpDir := t.TempDir()
	recordStore, err := storage.NewRecordStore(filepath.Join(tmpDir, "records.json"))
	if err != nil {
		t.Fatalf("new record store: %v", err)
	}
	settingsStore, err := storage.NewSettingsStore(filepath.Join(tmpDir, "settings.json"), storage.RuntimeSettings{
		AlertChatID: alertChatID,
		DMTemplate:  "你好",
	})
	if err != nil {
		t.Fatalf("new settings store: %v", err)
	}
	return recordStore, settingsStore
}

package tg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mtproto "github.com/gotd/td/tg"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
)

func TestAdminAuthCodePromptAndSubmit(t *testing.T) {
	t.Parallel()

	messageCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sendMessage" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload["chat_id"].(float64) != 123 {
			t.Fatalf("unexpected chat id: %+v", payload["chat_id"])
		}
		messageCh <- payload["text"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	t.Cleanup(server.Close)

	authFlow := NewAdminAuth("+123456", 123, NewClientWithBaseURL(server.URL), logx.New("debug"))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resultCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		code, err := authFlow.Code(ctx, &mtproto.AuthSentCode{})
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- code
	}()

	var message string
	select {
	case message = <-messageCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt message")
	}
	if !strings.Contains(message, "+123456") {
		t.Fatalf("expected prompt to mention phone, got %q", message)
	}

	handled, kind := authFlow.Submit("12345")
	if !handled || kind != "code" {
		t.Fatalf("expected code submission to be handled, got handled=%v kind=%q", handled, kind)
	}

	select {
	case err := <-errCh:
		t.Fatalf("code wait returned error: %v", err)
	case code := <-resultCh:
		if code != "12345" {
			t.Fatalf("expected code 12345, got %q", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for code result")
	}
}

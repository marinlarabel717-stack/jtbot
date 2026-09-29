package tg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gotd/td/telegram/auth"
	mtproto "github.com/gotd/td/tg"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
)

type pendingAuthKind string

const (
	pendingAuthCode     pendingAuthKind = "code"
	pendingAuthPassword pendingAuthKind = "password"
)

type pendingAuthRequest struct {
	kind pendingAuthKind
	resp chan string
}

type AdminAuth struct {
	phone       string
	adminUserID int64
	client      *Client
	logger      *logx.Logger

	mu      sync.Mutex
	pending *pendingAuthRequest
}

func NewAdminAuth(phone string, adminUserID int64, client *Client, logger *logx.Logger) *AdminAuth {
	return &AdminAuth{
		phone:       phone,
		adminUserID: adminUserID,
		client:      client,
		logger:      logger,
	}
}

func (a *AdminAuth) Phone(context.Context) (string, error) {
	return a.phone, nil
}

func (a *AdminAuth) Password(ctx context.Context) (string, error) {
	if password := strings.TrimSpace(os.Getenv("PASSWORD")); password != "" {
		return password, nil
	}
	return a.request(ctx, pendingAuthPassword, fmt.Sprintf("监控号 %s 需要两步验证密码，请直接在这里发送密码。", a.phone))
}

func (a *AdminAuth) AcceptTermsOfService(context.Context, mtproto.HelpTermsOfService) error {
	return errors.New("sign up flow is not supported in admin auth")
}

func (a *AdminAuth) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign up flow is not supported in admin auth")
}

func (a *AdminAuth) Code(ctx context.Context, *mtproto.AuthSentCode) (string, error) {
	return a.request(ctx, pendingAuthCode, fmt.Sprintf("监控号 %s 需要登录验证码，请直接在这里发送本次验证码。", a.phone))
}

func (a *AdminAuth) Submit(text string) (bool, string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return false, ""
	}

	a.mu.Lock()
	pending := a.pending
	a.mu.Unlock()
	if pending == nil {
		return false, ""
	}

	select {
	case pending.resp <- text:
		return true, string(pending.kind)
	default:
		return false, ""
	}
}

func (a *AdminAuth) request(ctx context.Context, kind pendingAuthKind, prompt string) (string, error) {
	if a.client == nil || a.adminUserID == 0 {
		return "", errors.New("admin auth backend is not configured")
	}

	req := &pendingAuthRequest{
		kind: kind,
		resp: make(chan string, 1),
	}

	a.mu.Lock()
	a.pending = req
	a.mu.Unlock()

	if err := a.client.SendMessage(ctx, a.adminUserID, prompt, nil); err != nil {
		a.clearPending(req)
		return "", err
	}
	if a.logger != nil {
		a.logger.Infof("waiting for admin auth %s input", kind)
	}

	select {
	case <-ctx.Done():
		a.clearPending(req)
		return "", ctx.Err()
	case value := <-req.resp:
		a.clearPending(req)
		return strings.TrimSpace(value), nil
	}
}

func (a *AdminAuth) clearPending(req *pendingAuthRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending == req {
		a.pending = nil
	}
}

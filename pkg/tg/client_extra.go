package tg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	mtproto "github.com/gotd/td/tg"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type botAPIFile struct {
	FilePath string `json:"file_path"`
}

func (c *Client) DownloadFile(ctx context.Context, fileID string) (string, []byte, error) {
	if c.baseURL == "" {
		return "", nil, errors.New("downloadFile is only supported in bot api mode")
	}
	fileID = strings.TrimSpace(fileID)
	if fileID == "" {
		return "", nil, errors.New("file_id is empty")
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}

	payload := map[string]any{"file_id": fileID}
	var response apiResponse[botAPIFile]
	if err := c.doJSONRequest(ctx, http.MethodPost, "getFile", payload, &response); err != nil {
		return "", nil, err
	}
	if !response.OK {
		return "", nil, fmt.Errorf("compat getFile failed: %s", response.Description)
	}
	if strings.TrimSpace(response.Result.FilePath) == "" {
		return "", nil, errors.New("telegram returned empty file_path")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.fileDownloadURL(response.Result.FilePath), nil)
	if err != nil {
		return "", nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return "", nil, fmt.Errorf("download file http %d: %s", resp.StatusCode, string(data))
	}

	return filepath.Base(response.Result.FilePath), data, nil
}

func (c *Client) ProbeSession(ctx context.Context) (*model.User, error) {
	if c.baseURL != "" {
		return nil, errors.New("probeSession is only supported in user-session mode")
	}
	if err := os.MkdirAll(filepath.Dir(c.sessionFile), 0o755); err != nil {
		return nil, fmt.Errorf("create session dir: %w", err)
	}

	sessionStorage := &telegram.FileSessionStorage{Path: c.sessionFile}
	client := telegram.NewClient(c.appID, c.appHash, telegram.Options{
		SessionStorage: sessionStorage,
	})
	flow := auth.NewFlow(importOnlyAuth{}, auth.SendCodeOptions{})

	var selfUser *model.User
	err := client.Run(ctx, func(ctx context.Context) error {
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth: %w", err)
		}

		self, err := client.Self(ctx)
		if err != nil {
			return fmt.Errorf("get self: %w", err)
		}
		selfUser = mapTelegramUser(self)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if selfUser == nil {
		return nil, errors.New("probeSession returned empty self")
	}
	return selfUser, nil
}

func (c *Client) fileDownloadURL(filePath string) string {
	base := strings.TrimRight(c.baseURL, "/")
	filePath = strings.TrimLeft(filePath, "/")
	if idx := strings.Index(base, "/bot"); idx >= 0 {
		return base[:idx] + "/file" + base[idx:] + "/" + filePath
	}
	return base + "/file/" + filePath
}

type importOnlyAuth struct{}

func (importOnlyAuth) Phone(context.Context) (string, error) {
	return "", errors.New("session requires re-authentication")
}

func (importOnlyAuth) Password(context.Context) (string, error) {
	return "", errors.New("session requires re-authentication")
}

func (importOnlyAuth) AcceptTermsOfService(context.Context, mtproto.HelpTermsOfService) error {
	return errors.New("sign up flow is not supported")
}

func (importOnlyAuth) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign up flow is not supported")
}

func (importOnlyAuth) Code(context.Context, *mtproto.AuthSentCode) (string, error) {
	return "", errors.New("session requires re-authentication")
}

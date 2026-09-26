package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description"`
}

func NewClient(token string) *Client {
	return NewClientWithBaseURL("https://api.telegram.org/bot" + token + "/")
}

func NewClientWithBaseURL(baseURL string) *Client {
	if baseURL != "" && baseURL[len(baseURL)-1] != '/' {
		baseURL += "/"
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

func (c *Client) GetUpdates(ctx context.Context, offset, timeout int) ([]model.Update, error) {
	query := url.Values{}
	query.Set("offset", strconv.Itoa(offset))
	query.Set("timeout", strconv.Itoa(timeout))

	var response apiResponse[[]model.Update]
	if err := c.doJSONRequest(ctx, http.MethodGet, "getUpdates?"+query.Encode(), nil, &response); err != nil {
		return nil, err
	}
	if !response.OK {
		return nil, fmt.Errorf("telegram getUpdates failed: %s", response.Description)
	}
	return response.Result, nil
}

func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
	payload := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}

	var response apiResponse[map[string]any]
	if err := c.doJSONRequest(ctx, http.MethodPost, "sendMessage", payload, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("telegram sendMessage failed: %s", response.Description)
	}
	return nil
}

func (c *Client) doJSONRequest(ctx context.Context, method, endpoint string, payload any, result any) error {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("telegram http %d: %s", resp.StatusCode, string(data))
	}

	if err := json.Unmarshal(data, result); err != nil {
		return err
	}
	return nil
}

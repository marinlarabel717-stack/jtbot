package tg

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/updates"
	updhook "github.com/gotd/td/telegram/updates/hook"
	mtproto "github.com/gotd/td/tg"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type Client struct {
	appID       int
	appHash     string
	phone       string
	sessionFile string
	authFlow    auth.UserAuthenticator
	logger      *logx.Logger
	baseURL     string
	httpClient  *http.Client

	mu     sync.RWMutex
	api    *mtproto.Client
	selfID int64
	peers  map[int64]mtproto.InputPeerClass
}

func NewClient(appID int, appHash, phone, sessionFile string, logger *logx.Logger) *Client {
	return NewClientWithAuth(appID, appHash, phone, sessionFile, terminalAuth{phone: phone}, logger)
}

func NewClientWithAuth(appID int, appHash, phone, sessionFile string, authFlow auth.UserAuthenticator, logger *logx.Logger) *Client {
	if authFlow == nil {
		authFlow = terminalAuth{phone: phone}
	}
	return &Client{
		appID:       appID,
		appHash:     appHash,
		phone:       phone,
		sessionFile: sessionFile,
		authFlow:    authFlow,
		logger:      logger,
		peers:       make(map[int64]mtproto.InputPeerClass),
	}
}

func NewBotAPIClient(token string) *Client {
	token = strings.TrimSpace(token)
	baseURL := ""
	if token != "" {
		baseURL = "https://api.telegram.org/bot" + token + "/"
	}
	return NewClientWithBaseURL(baseURL)
}

func NewClientWithBaseURL(baseURL string) *Client {
	if baseURL != "" && !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
		peers:      make(map[int64]mtproto.InputPeerClass),
	}
}

func (c *Client) Run(ctx context.Context, handler func(context.Context, model.Update) error) error {
	dispatcher := mtproto.NewUpdateDispatcher()
	gaps := updates.New(updates.Config{
		Handler: dispatcher,
	})

	dispatcher.OnNewMessage(func(ctx context.Context, entities mtproto.Entities, update *mtproto.UpdateNewMessage) error {
		return c.handleIncomingMessage(ctx, entities, update.Message, handler)
	})
	dispatcher.OnNewChannelMessage(func(ctx context.Context, entities mtproto.Entities, update *mtproto.UpdateNewChannelMessage) error {
		return c.handleIncomingMessage(ctx, entities, update.Message, handler)
	})

	if err := os.MkdirAll(filepath.Dir(c.sessionFile), 0o755); err != nil {
		return fmt.Errorf("create session dir: %w", err)
	}

	sessionStorage := &telegram.FileSessionStorage{Path: c.sessionFile}
	client := telegram.NewClient(c.appID, c.appHash, telegram.Options{
		SessionStorage: sessionStorage,
		UpdateHandler:  gaps,
		Middlewares: []telegram.Middleware{
			updhook.UpdateHook(gaps.Handle),
		},
	})

	flow := auth.NewFlow(c.authFlow, auth.SendCodeOptions{})

	return client.Run(ctx, func(ctx context.Context) error {
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			completeAuthFlow(c.authFlow)
			return fmt.Errorf("auth: %w", err)
		}
		completeAuthFlow(c.authFlow)

		self, err := client.Self(ctx)
		if err != nil {
			return fmt.Errorf("get self: %w", err)
		}

		api := client.API()

		c.mu.Lock()
		c.api = api
		c.selfID = self.ID
		c.peers[self.ID] = &mtproto.InputPeerSelf{}
		c.mu.Unlock()

		c.logger.Infof("authorized account id=%d phone=%s", self.ID, c.phone)

		return gaps.Run(ctx, api, self.ID, updates.AuthOptions{
			OnStart: func(context.Context) {
				c.logger.Infof("mtproto updates listener started")
			},
		})
	})
}

func (c *Client) GetUpdates(ctx context.Context, offset, timeout int) ([]model.Update, error) {
	if c.baseURL != "" {
		query := url.Values{}
		query.Set("offset", strconv.Itoa(offset))
		query.Set("timeout", strconv.Itoa(timeout))

		var response apiResponse[[]model.Update]
		if err := c.doJSONRequest(ctx, http.MethodGet, "getUpdates?"+query.Encode(), nil, &response); err != nil {
			return nil, err
		}
		if !response.OK {
			return nil, fmt.Errorf("compat getUpdates failed: %s", response.Description)
		}
		return response.Result, nil
	}
	return nil, errors.New("getUpdates is not supported in user-session mode")
}

func (c *Client) SendMessage(ctx context.Context, chatID int64, text string, replyMarkup *model.InlineKeyboardMarkup) error {
	if c.baseURL != "" {
		payload := map[string]any{
			"chat_id": chatID,
			"text":    text,
		}
		if replyMarkup != nil {
			payload["reply_markup"] = replyMarkup
		}
		var response apiResponse[map[string]any]
		if err := c.doJSONRequest(ctx, http.MethodPost, "sendMessage", payload, &response); err != nil {
			return err
		}
		if !response.OK {
			return fmt.Errorf("compat sendMessage failed: %s", response.Description)
		}
		return nil
	}

	if replyMarkup != nil {
		return errors.New("reply markup is not supported in user-session mode")
	}

	peer, err := c.lookupPeer(chatID)
	if err != nil {
		return err
	}

	return c.sendText(ctx, peer, text)
}

func (c *Client) SendDirectMessage(ctx context.Context, userID int64, username, text string) error {
	if c.baseURL != "" {
		return c.SendMessage(ctx, userID, text, nil)
	}

	peer, err := c.lookupPeer(userID)
	if err != nil && strings.TrimSpace(username) != "" {
		peer, err = c.resolveUsernamePeer(ctx, username)
	}
	if err != nil {
		return err
	}
	return c.sendText(ctx, peer, text)
}

func (c *Client) EditMessageText(ctx context.Context, chatID int64, messageID int64, text string, replyMarkup *model.InlineKeyboardMarkup) error {
	if c.baseURL != "" {
		payload := map[string]any{
			"chat_id":    chatID,
			"message_id": messageID,
			"text":       text,
		}
		if replyMarkup != nil {
			payload["reply_markup"] = replyMarkup
		}
		var response apiResponse[map[string]any]
		if err := c.doJSONRequest(ctx, http.MethodPost, "editMessageText", payload, &response); err != nil {
			return err
		}
		if !response.OK {
			return fmt.Errorf("compat editMessageText failed: %s", response.Description)
		}
		return nil
	}
	return errors.New("editMessageText is not supported in user-session mode")
}

func (c *Client) AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string) error {
	if c.baseURL != "" {
		payload := map[string]any{
			"callback_query_id": callbackQueryID,
			"text":              text,
		}
		var response apiResponse[bool]
		if err := c.doJSONRequest(ctx, http.MethodPost, "answerCallbackQuery", payload, &response); err != nil {
			return err
		}
		if !response.OK {
			return fmt.Errorf("compat answerCallbackQuery failed: %s", response.Description)
		}
		return nil
	}
	return errors.New("callback queries are not supported in user-session mode")
}

func (c *Client) SendDocument(ctx context.Context, chatID int64, filename string, data []byte, caption string) error {
	if c.baseURL == "" {
		return errors.New("sendDocument is only supported in bot api mode")
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.WriteField("chat_id", strconv.FormatInt(chatID, 10)); err != nil {
		return err
	}
	if strings.TrimSpace(caption) != "" {
		if err := writer.WriteField("caption", caption); err != nil {
			return err
		}
	}

	part, err := writer.CreateFormFile("document", filename)
	if err != nil {
		return err
	}
	if _, err := part.Write(data); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"sendDocument", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("compat http %d: %s", resp.StatusCode, string(respData))
	}

	var response apiResponse[map[string]any]
	if err := json.Unmarshal(respData, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("compat sendDocument failed: %s", response.Description)
	}
	return nil
}

func (c *Client) Label() string {
	if strings.TrimSpace(c.phone) != "" {
		return c.phone
	}
	if c.baseURL != "" {
		return "bot_api"
	}
	return "telegram"
}

func (c *Client) handleIncomingMessage(
	ctx context.Context,
	entities mtproto.Entities,
	message mtproto.MessageClass,
	handler func(context.Context, model.Update) error,
) error {
	msg, ok := message.(*mtproto.Message)
	if !ok || msg == nil || msg.Out {
		return nil
	}

	c.cacheEntities(entities)

	converted, ok := c.convertMessage(msg, entities)
	if !ok {
		return nil
	}

	return handler(ctx, model.Update{
		UpdateID: msg.ID,
		Message:  &converted,
	})
}

func (c *Client) convertMessage(msg *mtproto.Message, entities mtproto.Entities) (model.Message, bool) {
	fromUser := c.resolveUser(msg, entities)
	chat := c.resolveChat(msg.PeerID, entities)

	return model.Message{
		MessageID: int64(msg.ID),
		From:      fromUser,
		Chat:      chat,
		Text:      strings.TrimSpace(msg.Message),
		Date:      int64(msg.Date),
	}, true
}

func (c *Client) resolveUser(msg *mtproto.Message, entities mtproto.Entities) *model.User {
	if fromID, ok := msg.GetFromID(); ok {
		if peer, ok := fromID.(*mtproto.PeerUser); ok {
			if user, exists := entities.Users[peer.UserID]; exists {
				return mapTelegramUser(user)
			}
		}
	}

	if peer, ok := msg.PeerID.(*mtproto.PeerUser); ok {
		if user, exists := entities.Users[peer.UserID]; exists {
			return mapTelegramUser(user)
		}
	}

	return nil
}

func (c *Client) resolveChat(peer mtproto.PeerClass, entities mtproto.Entities) model.Chat {
	switch p := peer.(type) {
	case *mtproto.PeerChat:
		if chat, exists := entities.Chats[p.ChatID]; exists {
			return model.Chat{
				ID:    p.ChatID,
				Type:  "group",
				Title: chat.Title,
			}
		}
		return model.Chat{ID: p.ChatID, Type: "group"}
	case *mtproto.PeerChannel:
		if channel, exists := entities.Channels[p.ChannelID]; exists {
			username, _ := channel.GetUsername()
			return model.Chat{
				ID:       p.ChannelID,
				Type:     "channel",
				Title:    channel.Title,
				Username: username,
			}
		}
		return model.Chat{ID: p.ChannelID, Type: "channel"}
	case *mtproto.PeerUser:
		if user, exists := entities.Users[p.UserID]; exists {
			mapped := mapTelegramUser(user)
			title := strings.TrimSpace(strings.Join([]string{mapped.FirstName, mapped.LastName}, " "))
			if title == "" {
				title = mapped.Username
			}
			return model.Chat{
				ID:       p.UserID,
				Type:     "private",
				Title:    title,
				Username: mapped.Username,
			}
		}
		return model.Chat{ID: p.UserID, Type: "private"}
	default:
		return model.Chat{}
	}
}

func (c *Client) cacheEntities(entities mtproto.Entities) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, user := range entities.Users {
		if user == nil {
			continue
		}
		accessHash, ok := user.GetAccessHash()
		if !ok {
			continue
		}
		c.peers[user.ID] = &mtproto.InputPeerUser{
			UserID:     user.ID,
			AccessHash: accessHash,
		}
	}

	for _, chat := range entities.Chats {
		if chat == nil {
			continue
		}
		c.peers[chat.ID] = &mtproto.InputPeerChat{ChatID: chat.ID}
	}

	for _, channel := range entities.Channels {
		if channel == nil {
			continue
		}
		accessHash, ok := channel.GetAccessHash()
		if !ok {
			continue
		}
		c.peers[channel.ID] = &mtproto.InputPeerChannel{
			ChannelID:  channel.ID,
			AccessHash: accessHash,
		}
	}
}

func (c *Client) resolveUsernamePeer(ctx context.Context, username string) (mtproto.InputPeerClass, error) {
	c.mu.RLock()
	api := c.api
	c.mu.RUnlock()
	if api == nil {
		return nil, errors.New("telegram api is not ready")
	}

	username = strings.TrimSpace(strings.TrimPrefix(username, "@"))
	if username == "" {
		return nil, errors.New("username is empty")
	}

	result, err := api.ContactsResolveUsername(ctx, &mtproto.ContactsResolveUsernameRequest{
		Username: username,
	})
	if err != nil {
		return nil, err
	}

	users := result.MapUsers()
	chats := result.MapChats()
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, user := range users {
		u, ok := user.(*mtproto.User)
		if !ok || u == nil {
			continue
		}
		accessHash, ok := u.GetAccessHash()
		if !ok {
			continue
		}
		c.peers[u.ID] = &mtproto.InputPeerUser{
			UserID:     u.ID,
			AccessHash: accessHash,
		}
	}
	for _, chat := range chats {
		switch v := chat.(type) {
		case *mtproto.Channel:
			accessHash, ok := v.GetAccessHash()
			if !ok {
				continue
			}
			c.peers[v.ID] = &mtproto.InputPeerChannel{
				ChannelID:  v.ID,
				AccessHash: accessHash,
			}
		case *mtproto.Chat:
			c.peers[v.ID] = &mtproto.InputPeerChat{ChatID: v.ID}
		}
	}

	switch peer := result.Peer.(type) {
	case *mtproto.PeerUser:
		if resolved, ok := c.peers[peer.UserID]; ok {
			return resolved, nil
		}
	case *mtproto.PeerChannel:
		if resolved, ok := c.peers[peer.ChannelID]; ok {
			return resolved, nil
		}
	case *mtproto.PeerChat:
		if resolved, ok := c.peers[peer.ChatID]; ok {
			return resolved, nil
		}
	}
	return nil, fmt.Errorf("resolved username %s but peer is unavailable", username)
}

func (c *Client) lookupPeer(id int64) (mtproto.InputPeerClass, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if id == c.selfID {
		return &mtproto.InputPeerSelf{}, nil
	}

	peer, ok := c.peers[id]
	if !ok {
		return nil, fmt.Errorf("peer %d not cached yet; wait until this account sees that user/chat in updates", id)
	}
	return peer, nil
}

func (c *Client) sendText(ctx context.Context, peer mtproto.InputPeerClass, text string) error {
	c.mu.RLock()
	api := c.api
	c.mu.RUnlock()

	if api == nil {
		return errors.New("telegram api is not ready")
	}

	randomID, err := randomInt64()
	if err != nil {
		return err
	}

	_, err = api.MessagesSendMessage(ctx, &mtproto.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		RandomID: randomID,
	})
	return err
}

func mapTelegramUser(user *mtproto.User) *model.User {
	if user == nil {
		return nil
	}

	username, _ := user.GetUsername()
	firstName, _ := user.GetFirstName()
	lastName, _ := user.GetLastName()
	langCode, _ := user.GetLangCode()

	return &model.User{
		ID:           user.ID,
		IsBot:        user.Bot,
		FirstName:    firstName,
		LastName:     lastName,
		Username:     username,
		LanguageCode: langCode,
		HasAvatar:    user.Photo != nil,
	}
}

func randomInt64() (int64, error) {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("generate random id: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}

type terminalAuth struct {
	phone string
}

func (t terminalAuth) Phone(_ context.Context) (string, error) {
	return t.phone, nil
}

func (t terminalAuth) Password(ctx context.Context) (string, error) {
	if password := strings.TrimSpace(os.Getenv("PASSWORD")); password != "" {
		return password, nil
	}
	return promptTerminal("password (leave blank if not enabled): ")
}

func (t terminalAuth) AcceptTermsOfService(_ context.Context, _ mtproto.HelpTermsOfService) error {
	return errors.New("sign up flow is not supported in terminal auth")
}

func (t terminalAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign up flow is not supported in terminal auth")
}

func (t terminalAuth) Code(_ context.Context, _ *mtproto.AuthSentCode) (string, error) {
	return promptTerminal("code: ")
}

func promptTerminal(label string) (string, error) {
	fmt.Print(label)
	value, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(value), nil
}

func completeAuthFlow(authFlow auth.UserAuthenticator) {
	type completer interface {
		Complete()
	}
	if c, ok := authFlow.(completer); ok {
		c.Complete()
	}
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description"`
}

func (c *Client) doJSONRequest(ctx context.Context, method, endpoint string, payload any, result any) error {
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}

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
		return fmt.Errorf("compat http %d: %s", resp.StatusCode, string(data))
	}
	return json.Unmarshal(data, result)
}

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
	"time"
	"unicode/utf16"

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

func (c *Client) SendInlineBotResult(ctx context.Context, userID int64, username, botUsername, query string) error {
	if c.baseURL != "" {
		return errors.New("Bot API 模式不支持内联 Bot 结果发送")
	}

	query = strings.TrimSpace(query)
	if query == "" {
		return errors.New("PostBot 代码不能为空")
	}

	peer, err := c.lookupPeer(userID)
	if err != nil && strings.TrimSpace(username) != "" {
		peer, err = c.resolveUsernamePeer(ctx, username)
	}
	if err != nil {
		return err
	}

	bot, err := c.resolveUsernameUser(ctx, botUsername)
	if err != nil {
		return fmt.Errorf("无法定位内联 Bot：%w", err)
	}

	api, err := c.apiClient()
	if err != nil {
		return err
	}
	results, err := api.MessagesGetInlineBotResults(ctx, &mtproto.MessagesGetInlineBotResultsRequest{
		Bot:    bot,
		Peer:   peer,
		Query:  query,
		Offset: "",
	})
	if err != nil {
		return fmt.Errorf("获取 PostBot 结果失败：%w", err)
	}
	if len(results.Results) == 0 {
		return errors.New("PostBot 没有返回可发送结果，代码可能无效")
	}
	inlineResult, ok := results.Results[0].(interface{ GetID() string })
	if !ok || strings.TrimSpace(inlineResult.GetID()) == "" {
		return errors.New("PostBot 返回结果异常，缺少可发送的结果 ID")
	}

	randomID, err := randomInt64()
	if err != nil {
		return err
	}
	_, err = api.MessagesSendInlineBotResult(ctx, &mtproto.MessagesSendInlineBotResultRequest{
		Peer:     peer,
		RandomID: randomID,
		QueryID:  results.QueryID,
		ID:       inlineResult.GetID(),
		HideVia:  true,
	})
	if err != nil {
		return fmt.Errorf("发送 PostBot 结果失败：%w", err)
	}
	return nil
}

func (c *Client) ForwardMessageFromLink(ctx context.Context, userID int64, username, messageLink string, hideSource bool) error {
	if c.baseURL != "" {
		return errors.New("Bot API 模式不支持频道贴文转发")
	}

	sourceUsername, messageID, err := parseTelegramMessageLink(messageLink)
	if err != nil {
		return err
	}

	targetPeer, err := c.lookupPeer(userID)
	if err != nil && strings.TrimSpace(username) != "" {
		targetPeer, err = c.resolveUsernamePeer(ctx, username)
	}
	if err != nil {
		return err
	}

	sourcePeer, err := c.resolveUsernamePeer(ctx, sourceUsername)
	if err != nil {
		return fmt.Errorf("无法定位来源频道：%w", err)
	}

	randomID, err := randomInt64()
	if err != nil {
		return err
	}

	api, err := c.apiClient()
	if err != nil {
		return err
	}
	_, err = api.MessagesForwardMessages(ctx, &mtproto.MessagesForwardMessagesRequest{
		FromPeer:   sourcePeer,
		ID:         []int{messageID},
		RandomID:   []int64{randomID},
		ToPeer:     targetPeer,
		DropAuthor: hideSource,
	})
	if err != nil {
		return fmt.Errorf("转发频道贴文失败：%w", err)
	}
	return nil
}

func (c *Client) SendQuickReplyShortcut(ctx context.Context, userID int64, username string, shortcutID int) error {
	if c.baseURL != "" {
		return errors.New("Bot API 模式不支持企业快捷回复")
	}
	if shortcutID <= 0 {
		return errors.New("企业快捷回复 ID 必须大于 0")
	}

	peer, err := c.lookupPeer(userID)
	if err != nil && strings.TrimSpace(username) != "" {
		peer, err = c.resolveUsernamePeer(ctx, username)
	}
	if err != nil {
		return err
	}

	randomID, err := randomInt64()
	if err != nil {
		return err
	}

	api, err := c.apiClient()
	if err != nil {
		return err
	}
	_, err = api.MessagesSendQuickReplyMessages(ctx, &mtproto.MessagesSendQuickReplyMessagesRequest{
		Peer:       peer,
		ShortcutID: shortcutID,
		RandomID:   []int64{randomID},
	})
	if err != nil {
		return fmt.Errorf("发送企业快捷回复失败：%w", err)
	}
	return nil
}

func (c *Client) CheckSpamBotStatus(ctx context.Context) (string, string, bool, error) {
	if c.baseURL != "" {
		return "", "failed", false, errors.New("Bot API 模式不支持 SpamBot 检查")
	}

	peer, err := c.resolveUsernamePeer(ctx, "SpamBot")
	if err != nil {
		return "", "failed", false, fmt.Errorf("无法定位 @SpamBot：%w", err)
	}
	if err := c.sendText(ctx, peer, "/start"); err != nil {
		return "", "failed", false, fmt.Errorf("无法向 @SpamBot 发起检测：%w", err)
	}

	select {
	case <-ctx.Done():
		return "", "failed", false, ctx.Err()
	case <-time.After(2 * time.Second):
	}

	c.mu.RLock()
	api := c.api
	c.mu.RUnlock()
	if api == nil {
		return "", "failed", false, errors.New("telegram api is not ready")
	}

	history, err := api.MessagesGetHistory(ctx, &mtproto.MessagesGetHistoryRequest{
		Peer:  peer,
		Limit: 5,
	})
	if err != nil {
		return "", "failed", false, fmt.Errorf("读取 @SpamBot 回复失败：%w", err)
	}

	raw := latestIncomingText(history)
	if strings.TrimSpace(raw) == "" {
		return "未拿到 @SpamBot 的有效回复，请稍后再试", "unknown", false, nil
	}

	code, summary, canSend := interpretSpamBotStatus(raw)
	return summary, code, canSend, nil
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

func (c *Client) resolveUsernameUser(ctx context.Context, username string) (mtproto.InputUserClass, error) {
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

	for _, user := range result.MapUsers() {
		u, ok := user.(*mtproto.User)
		if !ok || u == nil {
			continue
		}
		resolvedUsername, _ := u.GetUsername()
		accessHash, ok := u.GetAccessHash()
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(resolvedUsername), username) || u.ID != 0 {
			return &mtproto.InputUser{
				UserID:     u.ID,
				AccessHash: accessHash,
			}, nil
		}
	}

	return nil, fmt.Errorf("resolved username %s but user is unavailable", username)
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

func (c *Client) apiClient() (*mtproto.Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.api == nil {
		return nil, errors.New("telegram api is not ready")
	}
	return c.api, nil
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

	req := &mtproto.MessagesSendMessageRequest{
		Peer:     peer,
		Message:  text,
		RandomID: randomID,
	}
	if entities := buildMessageEntities(text); len(entities) > 0 {
		req.SetEntities(entities)
	}

	_, err = api.MessagesSendMessage(ctx, req)
	return err
}

func latestIncomingText(history mtproto.MessagesMessagesClass) string {
	withMessages, ok := any(history).(interface {
		GetMessages() []mtproto.MessageClass
	})
	if !ok {
		return ""
	}
	for _, item := range withMessages.GetMessages() {
		message, ok := item.(*mtproto.Message)
		if !ok || message == nil || message.Out {
			continue
		}
		if text := strings.TrimSpace(message.Message); text != "" {
			return text
		}
	}
	return ""
}

func interpretSpamBotStatus(raw string) (string, string, bool) {
	text := normalizeSpamBotText(raw)

	switch {
	case containsAny(text, "some phone numbers may trigger a harsh response", "phone numbers may trigger"):
		return "active", "账号目前可以正常私信，但这个号段更容易触发风控，建议控制发送节奏", true
	case containsAny(text, "good news, no limits are currently applied", "you're free as a bird", "no limits", "free as a bird", "no restrictions", "all good", "account is free", "not limited"):
		return "active", "账号状态正常，目前没有私信限制", true
	case containsAny(text, "mutual contacts", "only people in your contacts", "only send messages to mutual contacts", "双向", "互相添加"):
		return "restricted", "账号目前只能给双向联系人发消息，不能正常私信陌生人", false
	case containsAny(text, "account is now limited until", "limited until", "moderators have confirmed the report", "users found your messages annoying", "will be automatically released", "temporarily limited"):
		return "restricted", "账号被临时限制，暂时不能正常私信", false
	case containsAny(text, "actions can trigger a harsh response from our anti-spam systems", "account was limited", "you will not be able to send messages"):
		return "spam", "账号触发了垃圾消息风控，当前不适合继续私信", false
	case containsAny(text, "permanently banned", "account has been frozen permanently", "permanently restricted", "banned permanently", "blocked for violations", "terms of service", "banned", "suspended"):
		return "banned", "账号已被永久限制或封禁，不能再用于私信", false
	case containsAny(text, "wait", "pending", "verification"):
		return "frozen", "账号处于等待验证或审核状态，暂时不能稳定私信", false
	default:
		return "unknown", "未能明确识别账号状态，请人工查看 @SpamBot 最新回复", false
	}
}

func normalizeSpamBotText(text string) string {
	replacer := strings.NewReplacer(
		"正常", "all good",
		"没有限制", "no limits",
		"无限制", "no limits",
		"永久封禁", "permanently banned",
		"限制", "limited",
		"暂时", "temporarily",
		"验证", "verification",
	)
	return strings.ToLower(replacer.Replace(strings.TrimSpace(text)))
}

func containsAny(text string, patterns ...string) bool {
	for _, pattern := range patterns {
		if strings.Contains(text, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

func parseTelegramMessageLink(raw string) (string, int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0, errors.New("频道消息链接不能为空")
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", 0, errors.New("频道消息链接格式不对")
	}
	path := strings.Trim(parsed.Path, "/")
	parts := strings.Split(path, "/")
	if parsed.Host != "t.me" && parsed.Host != "www.t.me" {
		return "", 0, errors.New("目前只支持公开频道的 t.me 消息链接")
	}
	if len(parts) < 2 || parts[0] == "c" {
		return "", 0, errors.New("目前只支持公开频道消息链接，例如 https://t.me/channel/123")
	}

	messageID, err := strconv.Atoi(parts[1])
	if err != nil || messageID <= 0 {
		return "", 0, errors.New("频道消息链接里的消息 ID 不合法")
	}

	return parts[0], messageID, nil
}

func buildMessageEntities(text string) []mtproto.MessageEntityClass {
	runes := []rune(text)
	if len(runes) == 0 {
		return nil
	}

	entities := make([]mtproto.MessageEntityClass, 0, 2)
	for i := 0; i < len(runes); i++ {
		if runes[i] != '@' {
			continue
		}
		if i > 0 && isMentionChar(runes[i-1]) {
			continue
		}

		j := i + 1
		for j < len(runes) && isMentionChar(runes[j]) {
			j++
		}
		if j == i+1 {
			continue
		}

		username := string(runes[i+1 : j])
		mention := "@" + username
		offset := utf16Len(string(runes[:i]))
		length := utf16Len(mention)
		entities = append(entities, &mtproto.MessageEntityTextURL{
			Offset: offset,
			Length: length,
			URL:    "https://t.me/" + username,
		})
		i = j - 1
	}

	return entities
}

func isMentionChar(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '_'
}

func utf16Len(text string) int {
	return len(utf16.Encode([]rune(text)))
}

func mapTelegramUser(user *mtproto.User) *model.User {
	if user == nil {
		return nil
	}

	username, _ := user.GetUsername()
	firstName, _ := user.GetFirstName()
	lastName, _ := user.GetLastName()
	phone, _ := user.GetPhone()
	langCode, _ := user.GetLangCode()

	return &model.User{
		ID:           user.ID,
		IsBot:        user.Bot,
		FirstName:    firstName,
		LastName:     lastName,
		Username:     username,
		Phone:        phone,
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

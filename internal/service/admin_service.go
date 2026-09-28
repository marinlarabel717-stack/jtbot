package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

const (
	callbackMain          = "admin:main"
	callbackKeywords      = "admin:keywords"
	callbackKeywordAdd    = "admin:keyword:add"
	callbackKeywordRemove = "admin:keyword:remove"
	callbackRules         = "admin:rules"
	callbackToggleMonitor = "admin:rule:toggle_monitor"
	callbackToggleDryRun  = "admin:rule:toggle_dryrun"
	callbackSetCooldown   = "admin:rule:set_cooldown"
	callbackSetTemplate   = "admin:rule:set_template"
	callbackSetMaxLength  = "admin:rule:set_max_length"
	callbackSetMinAge     = "admin:rule:set_min_age"
	callbackToggleNoName  = "admin:rule:toggle_no_username"
	callbackToggleNoPhoto = "admin:rule:toggle_no_avatar"
	callbackChats         = "admin:chats"
	callbackAddChat       = "admin:chat:add"
	callbackRemoveChat    = "admin:chat:remove"
	callbackSetAlertChat  = "admin:alert:set_current"
	callbackBlacklist     = "admin:blacklist"
	callbackUnblockUser   = "admin:blacklist:user:remove"
	callbackUnblockChat   = "admin:blacklist:chat:remove"
	callbackBlockUser     = "admin:blacklist:user:"
	callbackBlockChat     = "admin:blacklist:chat:"
)

type pendingAction string

const (
	pendingNone          pendingAction = ""
	pendingAddKeywords   pendingAction = "add_keywords"
	pendingRemoveKeyword pendingAction = "remove_keywords"
	pendingSetCooldown   pendingAction = "set_cooldown"
	pendingSetTemplate   pendingAction = "set_template"
	pendingSetMaxLength  pendingAction = "set_max_length"
	pendingSetMinAge     pendingAction = "set_min_age"
	pendingAddChatIDs    pendingAction = "add_chat_ids"
	pendingRemoveChatIDs pendingAction = "remove_chat_ids"
	pendingUnblockUsers  pendingAction = "unblock_users"
	pendingUnblockChats  pendingAction = "unblock_chats"
)

type AdminService struct {
	adminUserID  int64
	client       *tg.Client
	keywordStore *storage.KeywordStore
	settings     *storage.SettingsStore
	blacklist    *storage.BlacklistStore
	logger       *logx.Logger

	mu      sync.Mutex
	pending map[int64]pendingAction
}

func NewAdminService(adminUserID int64, client *tg.Client, keywordStore *storage.KeywordStore, settings *storage.SettingsStore, blacklist *storage.BlacklistStore, logger *logx.Logger) *AdminService {
	return &AdminService{
		adminUserID:  adminUserID,
		client:       client,
		keywordStore: keywordStore,
		settings:     settings,
		blacklist:    blacklist,
		logger:       logger,
		pending:      make(map[int64]pendingAction),
	}
}

func (s *AdminService) HandleUpdate(ctx context.Context, update model.Update) (bool, error) {
	if update.CallbackQuery != nil {
		return s.handleCallback(ctx, update.CallbackQuery)
	}
	if update.Message != nil {
		return s.handleMessage(ctx, update.Message)
	}
	return false, nil
}

func (s *AdminService) handleMessage(ctx context.Context, msg *model.Message) (bool, error) {
	if msg.From == nil || msg.From.ID != s.adminUserID {
		return false, nil
	}

	text := strings.TrimSpace(msg.Content())
	switch text {
	case "/start", "/menu":
		s.clearPending(msg.From.ID)
		return true, s.client.SendMessage(ctx, msg.Chat.ID, s.mainText(), s.mainKeyboard())
	case "/chatid":
		return true, s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("当前聊天 ID: %d", msg.Chat.ID), nil)
	}

	action := s.getPending(msg.From.ID)
	if action == pendingNone {
		return false, nil
	}

	s.clearPending(msg.From.ID)
	return true, s.handlePendingInput(ctx, msg, action, text)
}

func (s *AdminService) handleCallback(ctx context.Context, callback *model.CallbackQuery) (bool, error) {
	if callback.From == nil || callback.From.ID != s.adminUserID {
		return false, nil
	}
	if callback.Message == nil {
		return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "no message context")
	}

	var (
		text     string
		keyboard *model.InlineKeyboardMarkup
		alert    string
		err      error
	)

	switch callback.Data {
	case callbackMain:
		text, keyboard = s.mainText(), s.mainKeyboard()
	case callbackKeywords:
		text, keyboard = s.keywordsText(), s.keywordsKeyboard()
	case callbackKeywordAdd:
		s.setPending(callback.From.ID, pendingAddKeywords)
		text, keyboard = "发送要添加的关键词，多个用 | 分隔。", s.keywordsKeyboard()
		alert = "等待你发关键词"
	case callbackKeywordRemove:
		s.setPending(callback.From.ID, pendingRemoveKeyword)
		text, keyboard = "发送要删除的关键词，多个用 | 分隔。", s.keywordsKeyboard()
		alert = "等待你发要删除的关键词"
	case callbackRules:
		text, keyboard = s.rulesText(), s.rulesKeyboard()
	case callbackToggleMonitor:
		var enabled bool
		enabled, err = s.settings.ToggleMonitoring()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "监控已开启"
			} else {
				alert = "监控已关闭"
			}
		}
	case callbackToggleDryRun:
		var enabled bool
		enabled, err = s.settings.ToggleDryRun()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "已切到 dry-run"
			} else {
				alert = "已切到真实发送"
			}
		}
	case callbackSetCooldown:
		s.setPending(callback.From.ID, pendingSetCooldown)
		text, keyboard = "发送新的冷却分钟数，比如 1440。", s.rulesKeyboard()
		alert = "等待你发冷却分钟数"
	case callbackSetTemplate:
		s.setPending(callback.From.ID, pendingSetTemplate)
		text, keyboard = "发送新的私信模板。可用变量: {username} {chat_title} {keywords} {message}", s.rulesKeyboard()
		alert = "等待你发私信模板"
	case callbackSetMaxLength:
		s.setPending(callback.From.ID, pendingSetMaxLength)
		text, keyboard = "发送最大消息长度，填 0 表示不限制。", s.rulesKeyboard()
		alert = "等待你发最大消息长度"
	case callbackSetMinAge:
		s.setPending(callback.From.ID, pendingSetMinAge)
		text, keyboard = "发送最小账号年龄天数，填 0 表示不限制。", s.rulesKeyboard()
		alert = "等待你发账号年龄"
	case callbackToggleNoName:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoUsername()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "已开启无用户名过滤"
			} else {
				alert = "已关闭无用户名过滤"
			}
		}
	case callbackToggleNoPhoto:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoAvatar()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "已开启无头像过滤"
			} else {
				alert = "已关闭无头像过滤"
			}
		}
	case callbackChats:
		text, keyboard = s.chatsText(), s.chatsKeyboard()
	case callbackAddChat:
		s.setPending(callback.From.ID, pendingAddChatIDs)
		text, keyboard = "发送要添加的监听群 ID，多个用 | 分隔。", s.chatsKeyboard()
		alert = "等待你发群 ID"
	case callbackRemoveChat:
		s.setPending(callback.From.ID, pendingRemoveChatIDs)
		text, keyboard = "发送要移除的监听群 ID，多个用 | 分隔。", s.chatsKeyboard()
		alert = "等待你发要移除的群 ID"
	case callbackSetAlertChat:
		err = s.settings.SetAlertChatID(callback.Message.Chat.ID)
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			alert = "当前聊天已设为通知群"
		}
	case callbackBlacklist:
		text, keyboard = s.blacklistText(), s.blacklistKeyboard()
	case callbackUnblockUser:
		s.setPending(callback.From.ID, pendingUnblockUsers)
		text, keyboard = "发送要移出黑名单的用户 ID，多个用 | 分隔。", s.blacklistKeyboard()
		alert = "等待你发用户 ID"
	case callbackUnblockChat:
		s.setPending(callback.From.ID, pendingUnblockChats)
		text, keyboard = "发送要移出黑名单的群 ID，多个用 | 分隔。", s.blacklistKeyboard()
		alert = "等待你发群 ID"
	default:
		switch {
		case strings.HasPrefix(callback.Data, callbackBlockUser):
			alert, err = s.blockUserCallback(callback.Data)
		case strings.HasPrefix(callback.Data, callbackBlockChat):
			alert, err = s.blockChatCallback(callback.Data)
		default:
			return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "unknown action")
		}
	}

	if err == nil && text != "" {
		err = s.client.EditMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, keyboard)
	}

	answerText := alert
	if err != nil {
		answerText = "操作失败: " + err.Error()
	}
	if answerText == "" {
		answerText = "ok"
	}
	answerErr := s.client.AnswerCallbackQuery(ctx, callback.ID, answerText)
	if err != nil {
		return true, err
	}
	return true, answerErr
}

func (s *AdminService) handlePendingInput(ctx context.Context, msg *model.Message, action pendingAction, text string) error {
	switch action {
	case pendingAddKeywords:
		added, err := s.keywordStore.Add(splitKeywords(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已添加 %d 个关键词。\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingRemoveKeyword:
		removed, err := s.keywordStore.Remove(splitKeywords(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已删除 %d 个关键词。\n\n%s", removed, s.keywordsText()), s.keywordsKeyboard())
	case pendingSetCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "冷却分钟数必须是正整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "冷却时间已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTemplate:
		if strings.TrimSpace(text) == "" {
			return s.client.SendMessage(ctx, msg.Chat.ID, "模板不能为空。", s.rulesKeyboard())
		}
		if err := s.settings.SetDMTemplate(text); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "私信模板已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMaxLength:
		maxLength, err := parseNonNegativeInt(text)
		if err != nil || maxLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "最大消息长度必须是非负整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetMaxMessageLength(maxLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "最大消息长度已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinAge:
		days, err := parseNonNegativeInt(text)
		if err != nil || days < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "最小账号年龄必须是非负整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetMinAccountAgeDays(days); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "账号年龄过滤已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddChatIDs:
		added, err := s.settings.AddMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已添加 %d 个监听群。\n\n%s", added, s.chatsText()), s.chatsKeyboard())
	case pendingRemoveChatIDs:
		removed, err := s.settings.RemoveMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已移除 %d 个监听群。\n\n%s", removed, s.chatsText()), s.chatsKeyboard())
	case pendingUnblockUsers:
		removed, err := s.removeBlockedUsers(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已移出 %d 个黑名单用户。\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingUnblockChats:
		removed, err := s.removeBlockedChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已移出 %d 个黑名单群。\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	default:
		return nil
	}
}

func (s *AdminService) blockUserCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "黑名单未启用", nil
	}
	payload := strings.TrimPrefix(data, callbackBlockUser)
	parts := strings.SplitN(payload, ":", 2)
	userID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", err
	}
	username := ""
	if len(parts) == 2 {
		username = parts[1]
	}
	added, err := s.blacklist.AddUser(userID, username)
	if err != nil {
		return "", err
	}
	if !added {
		return "用户已在黑名单", nil
	}
	return "已拉黑用户", nil
}

func (s *AdminService) blockChatCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "黑名单未启用", nil
	}
	chatID, err := strconv.ParseInt(strings.TrimPrefix(data, callbackBlockChat), 10, 64)
	if err != nil {
		return "", err
	}
	added, err := s.blacklist.AddChat(chatID, "")
	if err != nil {
		return "", err
	}
	if !added {
		return "群已在黑名单", nil
	}
	return "已拉黑群", nil
}

func (s *AdminService) removeBlockedUsers(ids []int64) (int, error) {
	if s.blacklist == nil {
		return 0, nil
	}
	removed := 0
	for _, id := range ids {
		ok, err := s.blacklist.RemoveUser(id)
		if err != nil {
			return removed, err
		}
		if ok {
			removed++
		}
	}
	return removed, nil
}

func (s *AdminService) removeBlockedChats(ids []int64) (int, error) {
	if s.blacklist == nil {
		return 0, nil
	}
	removed := 0
	for _, id := range ids {
		ok, err := s.blacklist.RemoveChat(id)
		if err != nil {
			return removed, err
		}
		if ok {
			removed++
		}
	}
	return removed, nil
}

func (s *AdminService) mainText() string {
	state := s.settings.Snapshot()
	return fmt.Sprintf(
		"JTBot 后台\n\n监控: %s\n关键词: %d 个\n监听群: %d 个\n冷却: %d 分钟\nDry-run: %t",
		onOff(state.MonitoringEnabled),
		len(s.keywordStore.List()),
		len(state.MonitorChatIDs),
		state.CooldownMinutes,
		state.DryRun,
	)
}

func (s *AdminService) keywordsText() string {
	keywords := s.keywordStore.List()
	body := "暂无关键词"
	if len(keywords) > 0 {
		body = strings.Join(keywords, " | ")
	}
	return fmt.Sprintf("关键词管理\n\n当前共 %d 个：\n%s", len(keywords), body)
}

func (s *AdminService) rulesText() string {
	state := s.settings.Snapshot()
	alertChat := "未设置"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"规则配置\n\n监控开关: %s\n冷却时间: %d 分钟\nDry-run: %t\n通知群: %s\n最大消息长度: %s\n过滤无用户名: %s\n过滤无头像: %s\n最小账号年龄: %s 天\n模板预览:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		state.DryRun,
		alertChat,
		formatOptionalNumber(state.MaxMessageLength, "不限"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "不限"),
		state.DMTemplate,
	)
}

func (s *AdminService) chatsText() string {
	state := s.settings.Snapshot()
	body := "暂无监听群"
	if len(state.MonitorChatIDs) > 0 {
		parts := make([]string, 0, len(state.MonitorChatIDs))
		for _, id := range state.MonitorChatIDs {
			parts = append(parts, strconv.FormatInt(id, 10))
		}
		body = strings.Join(parts, "\n")
	}
	return fmt.Sprintf("监听群配置\n\n当前共 %d 个：\n%s", len(state.MonitorChatIDs), body)
}

func (s *AdminService) blacklistText() string {
	if s.blacklist == nil {
		return "黑名单未启用"
	}

	users := s.blacklist.Users()
	userBody := "暂无黑名单用户"
	if len(users) > 0 {
		parts := make([]string, 0, len(users))
		for _, user := range users {
			line := strconv.FormatInt(user.UserID, 10)
			if user.Username != "" {
				line += " @" + user.Username
			}
			parts = append(parts, line)
		}
		userBody = strings.Join(parts, "\n")
	}

	chats := s.blacklist.Chats()
	chatBody := "暂无黑名单群"
	if len(chats) > 0 {
		parts := make([]string, 0, len(chats))
		for _, chat := range chats {
			line := strconv.FormatInt(chat.ChatID, 10)
			if chat.Title != "" {
				line += " " + chat.Title
			}
			parts = append(parts, line)
		}
		chatBody = strings.Join(parts, "\n")
	}

	return fmt.Sprintf("黑名单\n\n用户 %d 个：\n%s\n\n群 %d 个：\n%s", len(users), userBody, len(chats), chatBody)
}

func (s *AdminService) mainKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "关键词管理", CallbackData: callbackKeywords}},
			{{Text: "规则配置", CallbackData: callbackRules}},
			{{Text: "监听群配置", CallbackData: callbackChats}},
			{{Text: "黑名单", CallbackData: callbackBlacklist}},
		},
	}
}

func (s *AdminService) keywordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "添加关键词", CallbackData: callbackKeywordAdd}, {Text: "删除关键词", CallbackData: callbackKeywordRemove}},
			{{Text: "返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) rulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "开关监控", CallbackData: callbackToggleMonitor}, {Text: "切换 Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "设置冷却", CallbackData: callbackSetCooldown}, {Text: "设置模板", CallbackData: callbackSetTemplate}},
			{{Text: "最大消息长度", CallbackData: callbackSetMaxLength}, {Text: "最小账号年龄", CallbackData: callbackSetMinAge}},
			{{Text: "过滤无用户名", CallbackData: callbackToggleNoName}, {Text: "过滤无头像", CallbackData: callbackToggleNoPhoto}},
			{{Text: "当前聊天设为通知群", CallbackData: callbackSetAlertChat}},
			{{Text: "返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) chatsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "添加监听群", CallbackData: callbackAddChat}, {Text: "移除监听群", CallbackData: callbackRemoveChat}},
			{{Text: "返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) blacklistKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "移出黑名单用户", CallbackData: callbackUnblockUser}, {Text: "移出黑名单群", CallbackData: callbackUnblockChat}},
			{{Text: "返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) setPending(userID int64, action pendingAction) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[userID] = action
}

func (s *AdminService) getPending(userID int64) pendingAction {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending[userID]
}

func (s *AdminService) clearPending(userID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, userID)
}

func splitKeywords(text string) []string {
	text = strings.ReplaceAll(text, "\n", "|")
	parts := strings.Split(text, "|")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func parseInt64Parts(text string) []int64 {
	text = strings.ReplaceAll(text, "\n", "|")
	parts := strings.Split(text, "|")
	result := make([]int64, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err == nil && value != 0 {
			result = append(result, value)
		}
	}
	return result
}

func parseNonNegativeInt(text string) (int, error) {
	return strconv.Atoi(strings.TrimSpace(text))
}

func onOff(v bool) string {
	if v {
		return "开启"
	}
	return "关闭"
}

func formatOptionalNumber(value int, disabled string) string {
	if value <= 0 {
		return disabled
	}
	return strconv.Itoa(value)
}

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
)

const (
	callbackMain          = "admin:main"
	callbackAccounts      = "admin:accounts"
	callbackAccountLogin  = "admin:account:login"
	callbackKeywords      = "admin:keywords"
	callbackKeywordAdd    = "admin:keyword:add"
	callbackKeywordRemove = "admin:keyword:remove"
	callbackFilters       = "admin:filters"
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
	callbackExport        = "admin:export"
	callbackStatus        = "admin:status"
	callbackHelp          = "admin:help"
)

type pendingAction string

const (
	pendingNone          pendingAction = ""
	pendingLoginMonitor  pendingAction = "login_monitor"
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

type AuthSubmitter interface {
	Submit(text string) (bool, string)
}

type MonitorLoginManager interface {
	StartMonitorLogin(ctx context.Context, phone string) (string, error)
	MonitorSummary() string
	MonitorCounts() (active int, total int)
}

type AdminService struct {
	adminUserID    int64
	client         messageClient
	authInput      AuthSubmitter
	monitorManager MonitorLoginManager
	keywordStore   *storage.KeywordStore
	settings       *storage.SettingsStore
	blacklist      *storage.BlacklistStore
	logger         *logx.Logger

	mu      sync.Mutex
	pending map[int64]pendingAction
}

type messageClient interface {
	SendMessage(ctx context.Context, chatID int64, text string, replyMarkup *model.InlineKeyboardMarkup) error
	EditMessageText(ctx context.Context, chatID int64, messageID int64, text string, replyMarkup *model.InlineKeyboardMarkup) error
	AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string) error
}

func NewAdminService(
	adminUserID int64,
	client messageClient,
	keywordStore *storage.KeywordStore,
	settings *storage.SettingsStore,
	blacklist *storage.BlacklistStore,
	authInput AuthSubmitter,
	monitorManager MonitorLoginManager,
	logger *logx.Logger,
) *AdminService {
	return &AdminService{
		adminUserID:    adminUserID,
		client:         client,
		authInput:      authInput,
		monitorManager: monitorManager,
		keywordStore:   keywordStore,
		settings:       settings,
		blacklist:      blacklist,
		logger:         logger,
		pending:        make(map[int64]pendingAction),
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

	if s.authInput != nil && !strings.HasPrefix(text, "/") {
		if handled, kind := s.authInput.Submit(text); handled {
			ack := "已收到登录验证码，正在继续登录。"
			if kind == "password" {
				ack = "已收到两步验证密码，正在继续登录。"
			}
			return true, s.client.SendMessage(ctx, msg.Chat.ID, ack, nil)
		}
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
		return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "没有消息上下文")
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
	case callbackAccounts:
		text, keyboard = s.accountsText(), s.accountsKeyboard()
	case callbackAccountLogin:
		s.setPending(callback.From.ID, pendingLoginMonitor)
		text, keyboard = s.accountLoginPrompt(), s.accountsKeyboard()
		alert = "把手机号直接发给我就行"
	case callbackKeywords:
		text, keyboard = s.keywordsText(), s.keywordsKeyboard()
	case callbackKeywordAdd:
		s.setPending(callback.From.ID, pendingAddKeywords)
		text, keyboard = "发送要添加的关键词，多个用 | 或换行分隔。", s.keywordsKeyboard()
		alert = "等待你发送关键词"
	case callbackKeywordRemove:
		s.setPending(callback.From.ID, pendingRemoveKeyword)
		text, keyboard = "发送要删除的关键词，多个用 | 或换行分隔。", s.keywordsKeyboard()
		alert = "等待你发送要删除的关键词"
	case callbackFilters, callbackRules:
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
		alert = "等待你发送冷却分钟数"
	case callbackSetTemplate:
		s.setPending(callback.From.ID, pendingSetTemplate)
		text, keyboard = "发送新的私信模板。可用变量: {username} {chat_title} {keywords} {message}", s.rulesKeyboard()
		alert = "等待你发送私信模板"
	case callbackSetMaxLength:
		s.setPending(callback.From.ID, pendingSetMaxLength)
		text, keyboard = "发送最大消息长度，填 0 表示不限制。", s.rulesKeyboard()
		alert = "等待你发送最大消息长度"
	case callbackSetMinAge:
		s.setPending(callback.From.ID, pendingSetMinAge)
		text, keyboard = "发送最小账号年龄天数，填 0 表示不限制。", s.rulesKeyboard()
		alert = "等待你发送账号年龄"
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
		text, keyboard = "发送要添加的监听群 ID，多个用 | 或换行分隔。", s.chatsKeyboard()
		alert = "等待你发送群 ID"
	case callbackRemoveChat:
		s.setPending(callback.From.ID, pendingRemoveChatIDs)
		text, keyboard = "发送要移除的监听群 ID，多个用 | 或换行分隔。", s.chatsKeyboard()
		alert = "等待你发送要移除的群 ID"
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
		text, keyboard = "发送要移出黑名单的用户 ID，多个用 | 或换行分隔。", s.blacklistKeyboard()
		alert = "等待你发送用户 ID"
	case callbackUnblockChat:
		s.setPending(callback.From.ID, pendingUnblockChats)
		text, keyboard = "发送要移出黑名单的群 ID，多个用 | 或换行分隔。", s.blacklistKeyboard()
		alert = "等待你发送群 ID"
	case callbackExport:
		text, keyboard = s.exportText(), s.exportKeyboard()
	case callbackStatus:
		text, keyboard = s.statusText(), s.statusKeyboard()
	case callbackHelp:
		text, keyboard = s.helpText(), s.helpKeyboard()
	default:
		switch {
		case strings.HasPrefix(callback.Data, callbackBlockUser):
			alert, err = s.blockUserCallback(callback.Data)
		case strings.HasPrefix(callback.Data, callbackBlockChat):
			alert, err = s.blockChatCallback(callback.Data)
		default:
			return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "未知操作")
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
	case pendingLoginMonitor:
		phones := splitInputParts(text)
		if len(phones) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "手机号不能为空。", s.accountsKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "一次先登录一个监控号。多个监控号就重复点击“登录监控账号”依次添加。", s.accountsKeyboard())
		}
		if s.monitorManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "当前没有可用的监控账号管理器。", s.accountsKeyboard())
		}
		result, err := s.monitorManager.StartMonitorLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "启动登录失败: "+err.Error(), s.accountsKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.accountsText(), s.accountsKeyboard())
	case pendingAddKeywords:
		added, err := s.keywordStore.Add(splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已添加 %d 个关键词。\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingRemoveKeyword:
		removed, err := s.keywordStore.Remove(splitInputParts(text))
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
	active, total := 0, 0
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
	}
	return fmt.Sprintf(
		"🤖 JTBot 关键词监控机器人\n\n📱 监控账号: 在线 %d / 共 %d\n🔑 关键词: %d个",
		active,
		total,
		len(s.keywordStore.List()),
	)
}

func (s *AdminService) accountsText() string {
	body := "还没有配置监控号。"
	if s.monitorManager != nil {
		body = s.monitorManager.MonitorSummary()
	}
	return "📱 账号管理\n\n" + body
}

func (s *AdminService) accountLoginPrompt() string {
	return "📱 登录监控账号\n\n直接发送手机号给我，例如：\n+66955305284\n\n想加多个账号时，重复点击这个按钮逐个添加。"
}

func (s *AdminService) keywordsText() string {
	keywords := s.keywordStore.List()
	body := "暂无关键词"
	if len(keywords) > 0 {
		body = strings.Join(keywords, " | ")
	}
	return fmt.Sprintf("📝 关键词管理\n\n当前共 %d 个：\n%s", len(keywords), body)
}

func (s *AdminService) rulesText() string {
	state := s.settings.Snapshot()
	alertChat := "未设置"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"⚙️ 过滤设置\n\n监控开关: %s\n冷却时间: %d 分钟\nDry-run: %t\n通知群: %s\n最大消息长度: %s\n过滤无用户名: %s\n过滤无头像: %s\n最小账号年龄: %s 天\n私信模板:\n%s",
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
	return fmt.Sprintf("👂 监听群配置\n\n当前共 %d 个：\n%s", len(state.MonitorChatIDs), body)
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

	return fmt.Sprintf("🚫 黑名单\n\n用户 %d 个：\n%s\n\n群 %d 个：\n%s", len(users), userBody, len(chats), chatBody)
}

func (s *AdminService) exportText() string {
	return "📦 数据导出\n\n当前版本先提供数据位置说明：\n- 关键词: configs/keywords.example.json\n- 运行设置: data/settings.json\n- 黑名单: data/blacklist.json\n- 命中记录: data/records.json\n\n如果你要我继续加成一键导出文件，也可以直接再说。"
}

func (s *AdminService) statusText() string {
	active, total := 0, 0
	summary := "暂无监控号"
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
		summary = s.monitorManager.MonitorSummary()
	}
	state := s.settings.Snapshot()
	return fmt.Sprintf(
		"📊 运行状态\n\n监控账号: 在线 %d / 共 %d\n监控开关: %s\nDry-run: %t\n关键词数: %d\n监听群数: %d\n\n%s",
		active,
		total,
		onOff(state.MonitoringEnabled),
		state.DryRun,
		len(s.keywordStore.List()),
		len(state.MonitorChatIDs),
		summary,
	)
}

func (s *AdminService) helpText() string {
	return "❓ 帮助\n\n1. 先点“账号管理”登录监控号。\n2. 监控号收到验证码或两步密码提示后，直接把内容发给我。\n3. 在“关键词管理”里增删关键词。\n4. 在“过滤设置”里调整监控规则、监听群和通知群。\n5. `/chatid` 可以查看当前聊天 ID。"
}

func (s *AdminService) mainKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "📱 账号管理", CallbackData: callbackAccounts},
				{Text: "📝 关键词管理", CallbackData: callbackKeywords},
			},
			{
				{Text: "⚙️ 过滤设置", CallbackData: callbackFilters},
				{Text: "📦 数据导出", CallbackData: callbackExport},
			},
			{
				{Text: "📊 运行状态", CallbackData: callbackStatus},
				{Text: "❓ 帮助", CallbackData: callbackHelp},
			},
		},
	}
}

func (s *AdminService) accountsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "➕ 登录监控账号", CallbackData: callbackAccountLogin}},
			{{Text: "⬅️ 返回主页", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) keywordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "➕ 添加关键词", CallbackData: callbackKeywordAdd}, {Text: "🗑 删除关键词", CallbackData: callbackKeywordRemove}},
			{{Text: "⬅️ 返回主页", CallbackData: callbackMain}},
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
			{{Text: "监听群配置", CallbackData: callbackChats}, {Text: "黑名单", CallbackData: callbackBlacklist}},
			{{Text: "当前聊天设为通知群", CallbackData: callbackSetAlertChat}},
			{{Text: "⬅️ 返回主页", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) chatsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "➕ 添加监听群", CallbackData: callbackAddChat}, {Text: "🗑 移除监听群", CallbackData: callbackRemoveChat}},
			{{Text: "⬅️ 返回过滤设置", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) blacklistKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "移出黑名单用户", CallbackData: callbackUnblockUser}, {Text: "移出黑名单群", CallbackData: callbackUnblockChat}},
			{{Text: "⬅️ 返回过滤设置", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) exportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "⬅️ 返回主页", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) statusKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "⬅️ 返回主页", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) helpKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "⬅️ 返回主页", CallbackData: callbackMain}},
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

func splitInputParts(text string) []string {
	text = strings.ReplaceAll(text, "\n", "|")
	text = strings.ReplaceAll(text, ",", "|")
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
	parts := splitInputParts(text)
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

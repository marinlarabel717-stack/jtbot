package service

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
)

const (
	callbackMain                = "admin:main"
	callbackAccounts            = "admin:accounts"
	callbackAccountAdd          = "admin:account:add"
	callbackAccountList         = "admin:account:list"
	callbackAccountDetailPrefix = "admin:account:detail:"
	callbackAccountRetryPrefix  = "admin:account:retry:"
	callbackAccountDeletePrefix = "admin:account:delete:"
	callbackKeywords            = "admin:keywords"
	callbackKeywordAdd          = "admin:keyword:add"
	callbackKeywordRemove       = "admin:keyword:remove"
	callbackDMPool              = "admin:dm_pool"
	callbackDMConnect           = "admin:dm:connect"
	callbackDMUpload            = "admin:dm:upload"
	callbackDMList              = "admin:dm:list"
	callbackDMDetailPrefix      = "admin:dm:detail:"
	callbackDMCheckPrefix       = "admin:dm:check:"
	callbackDMCheckAll          = "admin:dm:check_all"
	callbackDMExportAbnormal    = "admin:dm:export_abnormal"
	callbackDMKeepOnlyNormal    = "admin:dm:keep_only_normal"
	callbackDMRetryPrefix       = "admin:dm:retry:"
	callbackDMDeletePrefix      = "admin:dm:delete:"
	callbackDMTemplates         = "admin:dm:templates"
	callbackDMTemplateAdd       = "admin:dm:template:add"
	callbackDMTemplateRemove    = "admin:dm:template:remove"
	callbackDMRecords           = "admin:dm:records"
	callbackDMExportFailed      = "admin:dm:export_failed"
	callbackDMSettings          = "admin:dm:settings"
	callbackExport              = "admin:export"
	callbackExportByTime        = "admin:export:time"
	callbackExportByKeyword     = "admin:export:keyword"
	callbackExportAll           = "admin:export:all"
	callbackExportFormatUsers   = "admin:export:format:users"
	callbackExportFormatIDs     = "admin:export:format:ids"
	callbackExportFormatCSV     = "admin:export:format:csv"
	callbackFilters             = "admin:filters"
	callbackToggleMonitor       = "admin:rule:toggle_monitor"
	callbackToggleDryRun        = "admin:rule:toggle_dryrun"
	callbackSetCooldown         = "admin:rule:set_cooldown"
	callbackSetChatCooldown     = "admin:rule:set_chat_cooldown"
	callbackSetTextCooldown     = "admin:rule:set_text_cooldown"
	callbackSetTemplate         = "admin:rule:set_template"
	callbackSetMinLength        = "admin:rule:set_min_length"
	callbackSetMaxLength        = "admin:rule:set_max_length"
	callbackSetMinAge           = "admin:rule:set_min_age"
	callbackToggleNoName        = "admin:rule:toggle_no_username"
	callbackToggleNoPhoto       = "admin:rule:toggle_no_avatar"
	callbackChats               = "admin:chats"
	callbackAddChat             = "admin:chat:add"
	callbackRemoveChat          = "admin:chat:remove"
	callbackSetAlertChat        = "admin:alert:set_current"
	callbackBlacklist           = "admin:blacklist"
	callbackUnblockUser         = "admin:blacklist:user:remove"
	callbackUnblockChat         = "admin:blacklist:chat:remove"
	callbackBlockUser           = "admin:blacklist:user:"
	callbackBlockChat           = "admin:blacklist:chat:"
	callbackStatus              = "admin:status"
)

type pendingAction string

const (
	pendingNone             pendingAction = ""
	pendingLoginMonitor     pendingAction = "login_monitor"
	pendingLoginDM          pendingAction = "login_dm"
	pendingUploadDMSess     pendingAction = "upload_dm_session"
	pendingAddKeywords      pendingAction = "add_keywords"
	pendingAddKeywordsExact pendingAction = "add_keywords_exact"
	pendingAddKeywordsFuzzy pendingAction = "add_keywords_fuzzy"
	pendingRemoveKeyword    pendingAction = "remove_keywords"
	pendingSetCooldown      pendingAction = "set_cooldown"
	pendingSetChatCooldown  pendingAction = "set_chat_cooldown"
	pendingSetTextCooldown  pendingAction = "set_text_cooldown"
	pendingSetTemplate      pendingAction = "set_template"
	pendingAddDMTemplate    pendingAction = "add_dm_template"
	pendingRemoveDMTpl      pendingAction = "remove_dm_template"
	pendingSetMinLength     pendingAction = "set_min_length"
	pendingSetMaxLength     pendingAction = "set_max_length"
	pendingSetMinAge        pendingAction = "set_min_age"
	pendingAddChatIDs       pendingAction = "add_chat_ids"
	pendingRemoveChatIDs    pendingAction = "remove_chat_ids"
	pendingUnblockUsers     pendingAction = "unblock_users"
	pendingUnblockChats     pendingAction = "unblock_chats"
	pendingExportTime       pendingAction = "export_time"
	pendingExportKeyword    pendingAction = "export_keyword"
)

type exportFilterType string

const (
	exportAll     exportFilterType = "all"
	exportByTime  exportFilterType = "time"
	exportByWords exportFilterType = "keywords"
)

type exportContext struct {
	filterType exportFilterType
	start      time.Time
	end        time.Time
	keywords   []string
}

type AuthSubmitter interface {
	Submit(text string) (bool, string)
}

type MonitorAccountInfo struct {
	Phone       string
	SessionFile string
	Online      bool
	LastError   string
}

type DMAccountInfo struct {
	Phone           string
	SessionFile     string
	Online          bool
	LastError       string
	TodaySent       int
	TodaySuccess    int
	TodayFailed     int
	StatusCode      string
	StatusSummary   string
	StatusCheckedAt time.Time
	CanSendDM       bool
}

type DMAccountCheckResult struct {
	StatusCode string
	Summary    string
	CanSendDM  bool
	CheckedAt  time.Time
}

type MonitorLoginManager interface {
	StartMonitorLogin(ctx context.Context, phone string) (string, error)
	MonitorSummary() string
	MonitorCounts() (active int, total int)
	ListMonitorAccounts() []MonitorAccountInfo
	GetMonitorAccount(phone string) (MonitorAccountInfo, bool)
	RestartMonitor(ctx context.Context, phone string) error
	DeleteMonitor(ctx context.Context, phone string) error
}

type DMPoolManager interface {
	StartDMLogin(ctx context.Context, phone string) (string, error)
	ImportDMSessions(ctx context.Context, filename string, data []byte) (string, error)
	DMCounts() (active int, total int)
	ListDMAccounts() []DMAccountInfo
	GetDMAccount(phone string) (DMAccountInfo, bool)
	CheckDMAccount(ctx context.Context, phone string) (DMAccountCheckResult, error)
	RestartDMAccount(ctx context.Context, phone string) error
	DeleteDMAccount(ctx context.Context, phone string) error
}

type messageClient interface {
	SendMessage(ctx context.Context, chatID int64, text string, replyMarkup *model.InlineKeyboardMarkup) error
	EditMessageText(ctx context.Context, chatID int64, messageID int64, text string, replyMarkup *model.InlineKeyboardMarkup) error
	AnswerCallbackQuery(ctx context.Context, callbackQueryID, text string) error
	SendDocument(ctx context.Context, chatID int64, filename string, data []byte, caption string) error
}

type fileDownloader interface {
	DownloadFile(ctx context.Context, fileID string) (string, []byte, error)
}

type AdminService struct {
	adminUserID    int64
	client         messageClient
	authInput      AuthSubmitter
	monitorManager MonitorLoginManager
	dmManager      DMPoolManager
	keywordStore   *storage.KeywordStore
	settings       *storage.SettingsStore
	blacklist      *storage.BlacklistStore
	recordStore    *storage.RecordStore
	logger         *logx.Logger

	mu        sync.Mutex
	pending   map[int64]pendingAction
	exportCtx map[int64]exportContext
}

func NewAdminService(
	adminUserID int64,
	client messageClient,
	keywordStore *storage.KeywordStore,
	settings *storage.SettingsStore,
	blacklist *storage.BlacklistStore,
	authInput AuthSubmitter,
	monitorManager MonitorLoginManager,
	dmManager DMPoolManager,
	recordStore *storage.RecordStore,
	logger *logx.Logger,
) *AdminService {
	return &AdminService{
		adminUserID:    adminUserID,
		client:         client,
		authInput:      authInput,
		monitorManager: monitorManager,
		dmManager:      dmManager,
		keywordStore:   keywordStore,
		settings:       settings,
		blacklist:      blacklist,
		recordStore:    recordStore,
		logger:         logger,
		pending:        make(map[int64]pendingAction),
		exportCtx:      make(map[int64]exportContext),
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

	action := s.getPending(msg.From.ID)
	if action == pendingUploadDMSess {
		if msg.Document == nil {
			return true, s.client.SendMessage(ctx, msg.Chat.ID, "请发送 `.session` 或 `.zip` 文件。", s.dmPoolKeyboard())
		}
		err := s.handlePendingDMUpload(ctx, msg)
		if err == nil {
			s.clearPending(msg.From.ID)
		}
		return true, err
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
		text, keyboard = s.accountsOverviewText(), s.accountsMenuKeyboard()
	case callbackAccountAdd:
		s.setPending(callback.From.ID, pendingLoginMonitor)
		text, keyboard = s.accountAddPrompt(), s.backToAccountsKeyboard()
		alert = "把手机号直接发给我"
	case callbackAccountList:
		text, keyboard = s.accountsListText(), s.accountsListKeyboard()
	case callbackKeywords:
		text, keyboard = s.keywordsText(), s.keywordsKeyboard()
	case callbackKeywordAdd:
		s.setPending(callback.From.ID, pendingAddKeywordsFuzzy)
		text, keyboard = "发送要添加的模糊关键词，多个用 | 或换行分隔。\n\n模糊关键词：一句话里只要包含关键词就命中。", s.keywordsKeyboard()
		alert = "等待你发送模糊关键词"
	case callbackKeywordAdd + ":exact":
		s.setPending(callback.From.ID, pendingAddKeywordsExact)
		text, keyboard = "发送要添加的精准关键词，多个用 | 或换行分隔。\n\n精准关键词：消息内容必须与关键词完全一致才命中。", s.keywordsKeyboard()
		alert = "等待你发送精准关键词"
	case callbackKeywordRemove:
		s.setPending(callback.From.ID, pendingRemoveKeyword)
		text, keyboard = "发送要删除的关键词，多个用 | 或换行分隔。\n\n支持：\n精准:关键词\n模糊:关键词\n或直接发关键词文本（会删除同名的精准/模糊规则）。", s.keywordsKeyboard()
		alert = "等待你发送要删除的关键词"
	case callbackDMPool:
		text, keyboard = s.dmPoolText(), s.dmPoolKeyboard()
	case callbackDMConnect:
		s.setPending(callback.From.ID, pendingLoginDM)
		text, keyboard = s.dmConnectPrompt(), s.dmPoolKeyboard()
		alert = "把私信号手机号直接发给我"
	case callbackDMUpload:
		s.setPending(callback.From.ID, pendingUploadDMSess)
		text, keyboard = s.dmUploadPrompt(), s.dmPoolKeyboard()
		alert = "把 session 文件直接发给我"
	case callbackDMList:
		text, keyboard = s.dmAccountsText(), s.dmAccountsKeyboard()
	case callbackDMCheckAll:
		return true, s.handleDMCheckAll(ctx, callback)
	case callbackDMExportAbnormal:
		return true, s.handleDMExportAbnormal(ctx, callback)
	case callbackDMKeepOnlyNormal:
		return true, s.handleDMKeepOnlyNormal(ctx, callback)
	case callbackDMTemplates:
		text, keyboard = s.dmTemplatesText(), s.dmTemplatesKeyboard()
	case callbackDMTemplateAdd:
		s.setPending(callback.From.ID, pendingAddDMTemplate)
		text, keyboard = "发送要添加的话术模板，多条可用 --- 分隔。", s.dmTemplatesKeyboard()
		alert = "等待你发送话术模板"
	case callbackDMTemplateRemove:
		s.setPending(callback.From.ID, pendingRemoveDMTpl)
		text, keyboard = "发送要删除的话术内容，支持一次删除多条，使用 | 或换行分隔。", s.dmTemplatesKeyboard()
		alert = "等待你发送要删除的话术"
	case callbackDMRecords:
		text, keyboard = s.dmRecordsText(), s.dmRecordsKeyboard()
	case callbackDMExportFailed:
		err = s.sendFailedDMExport(ctx, callback.Message.Chat.ID)
		alert = "异常私信记录已导出"
		if err == nil {
			text, keyboard = s.dmRecordsText(), s.dmRecordsKeyboard()
		}
	case callbackDMSettings:
		text, keyboard = s.dmSettingsText(), s.dmSettingsKeyboard()
	case callbackExport:
		text, keyboard = s.exportText(), s.exportKeyboard()
	case callbackExportByTime:
		s.setPending(callback.From.ID, pendingExportTime)
		text, keyboard = s.exportTimePrompt(), s.cancelExportKeyboard()
	case callbackExportByKeyword:
		s.setPending(callback.From.ID, pendingExportKeyword)
		text, keyboard = s.exportKeywordPrompt(), s.cancelExportKeyboard()
	case callbackExportAll:
		s.setExportContext(callback.From.ID, exportContext{filterType: exportAll})
		text, keyboard = "已选择导出全部数据，请选择导出格式。", s.exportFormatKeyboard()
	case callbackExportFormatUsers, callbackExportFormatIDs, callbackExportFormatCSV:
		err = s.sendExportFile(ctx, callback.Message.Chat.ID, callback.From.ID, callback.Data)
		alert = "导出文件已发送"
		if err == nil {
			text, keyboard = s.exportText(), s.exportKeyboard()
		}
	case callbackFilters:
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
		text, keyboard = "发送新的同用户重复私信冷却分钟数，比如 1440。", s.rulesKeyboard()
		alert = "等待你发送冷却分钟数"
	case callbackSetChatCooldown:
		s.setPending(callback.From.ID, pendingSetChatCooldown)
		text, keyboard = "发送同群重复私信冷却分钟数，填 0 表示关闭。", s.rulesKeyboard()
		alert = "等待你发送同群冷却分钟数"
	case callbackSetTextCooldown:
		s.setPending(callback.From.ID, pendingSetTextCooldown)
		text, keyboard = "发送同内容重复私信冷却分钟数，填 0 表示关闭。", s.rulesKeyboard()
		alert = "等待你发送同内容冷却分钟数"
	case callbackSetTemplate:
		s.setPending(callback.From.ID, pendingSetTemplate)
		text, keyboard = "发送新的私信模板。可用变量: {username} {chat_title} {keywords} {message}", s.rulesKeyboard()
		alert = "等待你发送私信模板"
	case callbackSetMinLength:
		s.setPending(callback.From.ID, pendingSetMinLength)
		text, keyboard = "发送最小消息长度，填 0 表示不限制。", s.rulesKeyboard()
		alert = "等待你发送最小消息长度"
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
	case callbackStatus:
		text, keyboard = s.statusText(), s.statusKeyboard()
	default:
		switch {
		case strings.HasPrefix(callback.Data, callbackAccountDetailPrefix):
			phone := strings.TrimPrefix(callback.Data, callbackAccountDetailPrefix)
			text, keyboard, err = s.accountDetailText(phone)
		case strings.HasPrefix(callback.Data, callbackAccountRetryPrefix):
			phone := strings.TrimPrefix(callback.Data, callbackAccountRetryPrefix)
			err = s.monitorManager.RestartMonitor(ctx, phone)
			if err == nil {
				alert = "已重新发起连接"
				text, keyboard, _ = s.accountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackAccountDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackAccountDeletePrefix)
			err = s.monitorManager.DeleteMonitor(ctx, phone)
			if err == nil {
				alert = "监控号已删除"
				text, keyboard = s.accountsListText(), s.accountsListKeyboard()
			}
		case strings.HasPrefix(callback.Data, callbackDMDetailPrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMDetailPrefix)
			text, keyboard, err = s.dmAccountDetailText(phone)
		case strings.HasPrefix(callback.Data, callbackDMCheckPrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMCheckPrefix)
			var result DMAccountCheckResult
			result, err = s.dmManager.CheckDMAccount(ctx, phone)
			if err == nil {
				alert = result.Summary
				text, keyboard, _ = s.dmAccountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackDMRetryPrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMRetryPrefix)
			err = s.dmManager.RestartDMAccount(ctx, phone)
			if err == nil {
				alert = "已重新连接私信号"
				text, keyboard, _ = s.dmAccountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackDMDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMDeletePrefix)
			err = s.dmManager.DeleteDMAccount(ctx, phone)
			if err == nil {
				alert = "私信号已删除"
				text, keyboard = s.dmAccountsText(), s.dmAccountsKeyboard()
			}
		case strings.HasPrefix(callback.Data, callbackBlockUser):
			alert, err = s.blockUserCallback(callback.Data)
		case strings.HasPrefix(callback.Data, callbackBlockChat):
			alert, err = s.blockChatCallback(callback.Data)
		default:
			return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "未知操作")
		}
	}

	if err == nil && text != "" {
		err = s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, keyboard)
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
			return s.client.SendMessage(ctx, msg.Chat.ID, "手机号不能为空。", s.backToAccountsKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "一次先登录一个监控号。多个监控号请逐个添加。", s.backToAccountsKeyboard())
		}
		if s.monitorManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "当前没有可用的监控账号管理器。", s.backToAccountsKeyboard())
		}
		result, err := s.monitorManager.StartMonitorLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "启动登录失败: "+err.Error(), s.backToAccountsKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.accountsOverviewText(), s.accountsMenuKeyboard())
	case pendingLoginDM:
		phones := splitInputParts(text)
		if len(phones) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "手机号不能为空。", s.dmPoolKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "一次先登录一个私信号。多个私信号请逐个添加。", s.dmPoolKeyboard())
		}
		if s.dmManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "当前没有可用的私信号管理器。", s.dmPoolKeyboard())
		}
		result, err := s.dmManager.StartDMLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "启动私信号登录失败: "+err.Error(), s.dmPoolKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.dmPoolText(), s.dmPoolKeyboard())
	case pendingAddKeywordsExact:
		added, err := s.keywordStore.AddWithMode("exact", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已添加 %d 个精准关键词。\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingAddKeywordsFuzzy, pendingAddKeywords:
		added, err := s.keywordStore.AddWithMode("fuzzy", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已添加 %d 个模糊关键词。\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingRemoveKeyword:
		removed, err := s.keywordStore.Remove(splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已删除 %d 个关键词。\n\n%s", removed, s.keywordsText()), s.keywordsKeyboard())
	case pendingSetCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "同用户重复私信冷却必须是正整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "同用户重复私信冷却已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetChatCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "同群重复私信冷却必须是非负整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetChatCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "同群重复私信冷却已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTextCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "同内容重复私信冷却必须是非负整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetTextCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "同内容重复私信冷却已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTemplate:
		if strings.TrimSpace(text) == "" {
			return s.client.SendMessage(ctx, msg.Chat.ID, "模板不能为空。", s.rulesKeyboard())
		}
		if err := s.settings.SetDMTemplate(text); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "私信模板已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddDMTemplate:
		templates := splitTemplateParts(text)
		if len(templates) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "模板不能为空。", s.dmTemplatesKeyboard())
		}
		added, err := s.settings.AddDMTemplates(templates)
		if err != nil {
			return err
		}
		if added > 0 {
			_ = s.settings.SetDMTemplate(templates[0])
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已添加 %d 条话术。\n\n%s", added, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingRemoveDMTpl:
		removed, err := s.settings.RemoveDMTemplates(splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已删除 %d 条话术。\n\n%s", removed, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingSetMaxLength:
		maxLength, err := parseNonNegativeInt(text)
		if err != nil || maxLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "最大消息长度必须是非负整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetMaxMessageLength(maxLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "最大消息长度已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinLength:
		minLength, err := parseNonNegativeInt(text)
		if err != nil || minLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "最小消息长度必须是非负整数。", s.rulesKeyboard())
		}
		if err := s.settings.SetMinMessageLength(minLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "最小消息长度已更新。\n\n"+s.rulesText(), s.rulesKeyboard())
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
	case pendingExportTime:
		start, end, err := parseTimeRange(text)
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "时间格式不对。\n示例: 09-28-12:00 | 09-28-18:30", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByTime,
			start:      start,
			end:        end,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf(
			"已选择时间段：\n%s ~ %s\n\n请选择导出格式。",
			start.Format("01-02 15:04"),
			end.Format("01-02 15:04"),
		), s.exportFormatKeyboard())
	case pendingExportKeyword:
		keywords := splitInputParts(text)
		if len(keywords) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "关键词不能为空。", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByWords,
			keywords:   keywords,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, "已选择关键词："+strings.Join(keywords, ", ")+"\n\n请选择导出格式。", s.exportFormatKeyboard())
	default:
		return nil
	}
}

func (s *AdminService) handlePendingDMUpload(ctx context.Context, msg *model.Message) error {
	if msg.Document == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "请发送 `.session` 或 `.zip` 文件。", s.dmPoolKeyboard())
	}
	if s.dmManager == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "当前没有可用的私信号管理器。", s.dmPoolKeyboard())
	}

	filename := strings.TrimSpace(msg.Document.FileName)
	lowerName := strings.ToLower(filename)
	if !strings.HasSuffix(lowerName, ".session") && !strings.HasSuffix(lowerName, ".zip") {
		return s.client.SendMessage(ctx, msg.Chat.ID, "文件格式不支持，仅支持 `.session` 或 `.zip`。", s.dmPoolKeyboard())
	}

	downloader, ok := s.client.(fileDownloader)
	if !ok {
		return s.client.SendMessage(ctx, msg.Chat.ID, "当前 Bot API 客户端不支持下载文件。", s.dmPoolKeyboard())
	}

	if err := s.client.SendMessage(ctx, msg.Chat.ID, "开始下载并导入 Session，数量多时会稍等一会儿。", nil); err != nil {
		return err
	}

	downloadedName, data, err := downloader.DownloadFile(ctx, msg.Document.FileID)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "下载文件失败: "+err.Error(), s.dmPoolKeyboard())
	}
	if strings.TrimSpace(filename) == "" {
		filename = downloadedName
	}

	result, err := s.dmManager.ImportDMSessions(ctx, filename, data)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "导入 Session 失败: "+err.Error(), s.dmPoolKeyboard())
	}
	return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.dmPoolText(), s.dmPoolKeyboard())
}

func (s *AdminService) mainText() string {
	active, total := 0, 0
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
	}

	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	return fmt.Sprintf(
		"🤖 JTBot 关键词监控机器人\n\n📱 监控账号: %d在线 / %d离线\n🔑 关键词: %d个\n💬 私信记录: 发送 %d | 成功 %d | 失败 %d",
		active,
		total-active,
		len(s.keywordStore.List()),
		todaySent,
		todaySuccess,
		todayFailed,
	)
}

func (s *AdminService) accountsOverviewText() string {
	accounts := s.listMonitorAccounts()
	active, total := 0, len(accounts)
	for _, account := range accounts {
		if account.Online {
			active++
		}
	}
	return fmt.Sprintf("📱 监控账号管理\n\n已登录账号: %d\n在线: %d | 离线: %d", total, active, total-active)
}

func (s *AdminService) accountAddPrompt() string {
	return "请输入监控账号的手机号。\n\n支持格式：\n• +8613800138000\n• 8613800138000\n• +66955305284"
}

func (s *AdminService) accountsListText() string {
	accounts := s.listMonitorAccounts()
	if len(accounts) == 0 {
		return "❌ 暂无监控账号\n\n点击“添加新账号”开始添加。"
	}

	lines := []string{fmt.Sprintf("📋 账号列表 (%d个)：", len(accounts)), ""}
	for i, account := range accounts {
		status := "🔴 离线"
		if account.Online {
			status = "🟢 在线"
		}
		line := fmt.Sprintf("%d. %s %s", i+1, account.Phone, status)
		if account.LastError != "" && !account.Online {
			line += " | " + account.LastError
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) accountDetailText(phone string) (string, *model.InlineKeyboardMarkup, error) {
	account, ok := s.monitorManager.GetMonitorAccount(phone)
	if !ok {
		return "", nil, fmt.Errorf("监控号不存在")
	}

	status := "🔴 离线"
	if account.Online {
		status = "🟢 在线"
	}
	text := fmt.Sprintf("📱 账号详情\n\n手机号: %s\n状态: %s\nSession: %s", account.Phone, status, filepathBase(account.SessionFile))
	if strings.TrimSpace(account.LastError) != "" {
		text += "\n错误: " + account.LastError
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "🔄 重新连接", CallbackData: callbackAccountRetryPrefix + account.Phone},
				{Text: "❌ 删除账号", CallbackData: callbackAccountDeletePrefix + account.Phone},
			},
			{
				{Text: "🔙 返回列表", CallbackData: callbackAccountList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) keywordsText() string {
	keywords := s.keywordStore.ListEntries()
	if len(keywords) == 0 {
		return "📝 关键词列表为空"
	}
	lines := []string{fmt.Sprintf("📝 关键词列表 (%d个)：", len(keywords)), ""}
	const previewLimit = 60
	for i, keyword := range keywords {
		if i >= previewLimit {
			lines = append(lines, fmt.Sprintf("…… 还有 %d 个关键词未展开显示", len(keywords)-previewLimit))
			break
		}
		modeLabel := "模糊"
		if strings.EqualFold(strings.TrimSpace(keyword.Mode), "exact") {
			modeLabel = "精准"
		}
		lines = append(lines, fmt.Sprintf("%d. [%s] %s", i+1, modeLabel, keyword.Text))
	}
	lines = append(lines, "", "模糊：一句话里包含关键词就命中", "精准：整句内容必须与关键词完全一致才命中")
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmPoolText() string {
	active, total := 0, 0
	if s.dmManager != nil {
		active, total = s.dmManager.DMCounts()
	}
	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	templates := s.settings.ListDMTemplates()
	return fmt.Sprintf(
		"💬 私信号池\n\n已登录账号: %d\n在线: %d | 离线: %d\n话术模板: %d 条\n今日私信: 发送 %d | 成功 %d | 失败 %d\n\n支持两种接入方式：\n• 手动输入手机号登录\n• 上传 Telethon `.session` / `.zip` 批量导入",
		total,
		active,
		total-active,
		len(templates),
		todaySent,
		todaySuccess,
		todayFailed,
	)
}

func (s *AdminService) dmConnectPrompt() string {
	return "请输入私信号手机号。\n\n支持格式：\n• +8613800138000\n• 8613800138000\n• +66955305284"
}

func (s *AdminService) dmUploadPrompt() string {
	return "请发送 Telethon `.session` 文件，或一个包含多个 `.session` 的 `.zip` 压缩包。\n\n上传后会自动批量导入并尝试拉起私信号。"
}

func (s *AdminService) dmAccountsText() string {
	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return "❌ 暂无私信号\n\n点击“连接私信号”手动登录，或点“上传 Session”批量导入。"
	}

	lines := []string{fmt.Sprintf("📋 私信号列表 (%d个)：", len(accounts)), ""}
	for i, account := range accounts {
		status := "🔴 离线"
		if account.Online {
			status = "🟢 在线"
		}
		line := fmt.Sprintf("%d. %s %s | 今日 %d 条", i+1, account.Phone, status, account.TodaySent)
		if account.LastError != "" && !account.Online {
			line += " | " + account.LastError
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmAccountDetailText(phone string) (string, *model.InlineKeyboardMarkup, error) {
	account, ok := s.dmManager.GetDMAccount(phone)
	if !ok {
		return "", nil, fmt.Errorf("私信号不存在")
	}

	status := "🔴 离线"
	if account.Online {
		status = "🟢 在线"
	}
	text := fmt.Sprintf("💬 私信号详情\n\n手机号: %s\n状态: %s\nSession: %s\n今日发送: %d 条\n今日成功: %d 条\n今日失败: %d 条", account.Phone, status, filepathBase(account.SessionFile), account.TodaySent, account.TodaySuccess, account.TodayFailed)
	if strings.TrimSpace(account.LastError) != "" {
		text += "\n错误: " + account.LastError
	}
	if label := dmStatusLabel(account.StatusCode); label != "" {
		text += "\n账号限制: " + label
	}
	if strings.TrimSpace(account.StatusSummary) != "" {
		text += "\nSpamBot 检测: " + account.StatusSummary
		if !account.StatusCheckedAt.IsZero() {
			text += "\n检测时间: " + account.StatusCheckedAt.Format("2006-01-02 15:04:05")
		}
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "🔎 检查状态", CallbackData: callbackDMCheckPrefix + account.Phone},
				{Text: "🔄 重新连接", CallbackData: callbackDMRetryPrefix + account.Phone},
			},
			{
				{Text: "❌ 删除账号", CallbackData: callbackDMDeletePrefix + account.Phone},
				{Text: "🔙 返回列表", CallbackData: callbackDMList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) dmTemplatesText() string {
	templates := s.settings.ListDMTemplates()
	if len(templates) == 0 {
		return "📝 暂无私信话术模板"
	}

	lines := []string{fmt.Sprintf("📝 话术模板 (%d条)：", len(templates)), ""}
	for i, item := range templates {
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, item))
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmRecordsText() string {
	records := s.recordStore.DMRecords()
	if len(records) == 0 {
		return "📨 暂无私信记录"
	}

	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	lines := []string{
		fmt.Sprintf("📨 私信记录\n\n今日发送: %d | 成功 %d | 失败 %d", todaySent, todaySuccess, todayFailed),
		"",
		"最近 10 条：",
	}
	start := len(records) - 10
	if start < 0 {
		start = 0
	}
	for i := len(records) - 1; i >= start; i-- {
		record := records[i]
		target := strconv.FormatInt(record.UserID, 10)
		if strings.TrimSpace(record.Username) != "" {
			target = "@" + strings.TrimPrefix(record.Username, "@")
		}
		senderLabel := record.Sender
		if strings.TrimSpace(senderLabel) == "" {
			senderLabel = "-"
		}
		line := fmt.Sprintf("• %s | %s | via %s", target, record.Status, senderLabel)
		if strings.TrimSpace(record.Error) != "" {
			line += " | " + record.Error
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmSettingsText() string {
	state := s.settings.Snapshot()
	return fmt.Sprintf(
		"⚙️ 私信发送设置\n\n冷却时间: %d 分钟\n发送模式: %s\n当前默认模板:\n%s",
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		state.DMTemplate,
	)
}

func (s *AdminService) exportText() string {
	total := len(s.recordStore.MatchRecords())
	return fmt.Sprintf(
		"📤 数据导出\n\n当前命中记录: %d 条\n\n可导出：\n• 按时间段导出\n• 按关键词导出\n• 导出全部数据",
		total,
	)
}

func (s *AdminService) exportTimePrompt() string {
	return "📅 按时间段导出\n\n请输入时间范围：\n示例 1: 09-28-12:00 | 09-28-18:30\n示例 2: 2026-09-28 12:00 | 2026-09-28 18:30"
}

func (s *AdminService) exportKeywordPrompt() string {
	keywords := s.keywordStore.List()
	return "🔑 按关键词导出\n\n当前关键词：\n" + strings.Join(keywords, " | ") + "\n\n请输入要导出的关键词，多个用 | 分隔。"
}

func (s *AdminService) legacyRulesText() string {
	state := s.settings.Snapshot()
	alertChat := "未设置"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"⚙️ 过滤设置\n\n监控开关: %s\n冷却时间: %d 分钟\n发送模式: %s\n通知群: %s\n最大消息长度: %s\n过滤无用户名: %s\n过滤无头像: %s\n最小账号年龄: %s 天\n私信模板:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MaxMessageLength, "不限"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "不限"),
		state.DMTemplate,
	)
}

func (s *AdminService) rulesText() string {
	state := s.settings.Snapshot()
	alertChat := "未设置"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"⚙️ 过滤设置\n\n监控开关: %s\n同用户重复私信冷却: %d 分钟\n同群重复私信冷却: %s 分钟\n同内容重复私信冷却: %s 分钟\n发送模式: %s\n通知群: %s\n最小消息长度: %s\n最大消息长度: %s\n过滤无用户名: %s\n过滤无头像: %s\n最小账号年龄: %s 天\n私信模板:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		formatOptionalNumber(state.ChatCooldownMinutes, "关闭"),
		formatOptionalNumber(state.TextCooldownMinutes, "关闭"),
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MinMessageLength, "不限制"),
		formatOptionalNumber(state.MaxMessageLength, "不限制"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "不限制"),
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

func (s *AdminService) statusText() string {
	active, total := 0, 0
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
	}
	matchRecords := s.recordStore.MatchRecords()
	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	return fmt.Sprintf(
		"📊 运行状态\n\n监控账号: %d在线 / %d离线\n关键词: %d个\n命中记录: %d\n私信记录: 发送 %d | 成功 %d | 失败 %d\n过滤开关: %s\n发送模式: %s",
		active,
		total-active,
		len(s.keywordStore.List()),
		len(matchRecords),
		todaySent,
		todaySuccess,
		todayFailed,
		onOff(s.settings.IsMonitoringEnabled()),
		dryRunLabel(s.settings.IsDryRun()),
	)
}

func (s *AdminService) mainKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "📱 监控账号", CallbackData: callbackAccounts},
				{Text: "📝 关键词管理", CallbackData: callbackKeywords},
			},
			{
				{Text: "💬 私信号池", CallbackData: callbackDMPool},
				{Text: "📤 数据导出", CallbackData: callbackExport},
			},
			{
				{Text: "⚙️ 过滤设置", CallbackData: callbackFilters},
				{Text: "📊 运行状态", CallbackData: callbackStatus},
			},
		},
	}
}

func (s *AdminService) accountsMenuKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "➕ 添加新账号", CallbackData: callbackAccountAdd},
				{Text: "📋 账号列表", CallbackData: callbackAccountList},
			},
			{
				{Text: "🔙 返回主菜单", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) accountsListKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listMonitorAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+1)
	for _, account := range accounts {
		status := "🔴"
		if account.Online {
			status = "🟢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackAccountDetailPrefix + account.Phone},
		})
	}
	rows = append(rows, []model.InlineKeyboardButton{
		{Text: "🔙 返回", CallbackData: callbackAccounts},
	})
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) backToAccountsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "🔙 返回", CallbackData: callbackAccounts}},
		},
	}
}

func (s *AdminService) keywordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "➕ 模糊关键词", CallbackData: callbackKeywordAdd},
				{Text: "🎯 精准关键词", CallbackData: callbackKeywordAdd + ":exact"},
			},
			{
				{Text: "➖ 删除关键词", CallbackData: callbackKeywordRemove},
			},
			{
				{Text: "🔙 返回主菜单", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmPoolKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "🔌 连接私信号", CallbackData: callbackDMConnect},
				{Text: "📤 上传 Session", CallbackData: callbackDMUpload},
			},
			{
				{Text: "📋 账号列表", CallbackData: callbackDMList},
				{Text: "🔍 一键检查状态", CallbackData: callbackDMCheckAll},
			},
			{
				{Text: "📝 话术模板", CallbackData: callbackDMTemplates},
				{Text: "⚙️ 发送设置", CallbackData: callbackDMSettings},
			},
			{
				{Text: "📨 发送记录", CallbackData: callbackDMRecords},
			},
			{
				{Text: "🔙 返回主菜单", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmAccountsKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listDMAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+2)
	for _, account := range accounts {
		status := "🔴"
		if account.Online {
			status = "🟢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackDMDetailPrefix + account.Phone},
		})
	}
	rows = append(rows,
		[]model.InlineKeyboardButton{
			{Text: "🔌 连接私信号", CallbackData: callbackDMConnect},
			{Text: "📤 上传 Session", CallbackData: callbackDMUpload},
		},
		[]model.InlineKeyboardButton{{Text: "🔍 一键检查状态", CallbackData: callbackDMCheckAll}},
		[]model.InlineKeyboardButton{{Text: "🔙 返回", CallbackData: callbackDMPool}},
	)
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) dmTemplatesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "➕ 添加话术", CallbackData: callbackDMTemplateAdd},
				{Text: "➖ 删除话术", CallbackData: callbackDMTemplateRemove},
			},
			{
				{Text: "🔙 返回", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmRecordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "📤 导出异常号", CallbackData: callbackDMExportFailed},
			},
			{
				{Text: "🔙 返回", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmSettingsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "切换 Dry-run", CallbackData: callbackToggleDryRun},
				{Text: "设置冷却", CallbackData: callbackSetCooldown},
			},
			{
				{Text: "🔙 返回", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) exportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "📅 按时间段导出", CallbackData: callbackExportByTime}},
			{{Text: "🔑 按关键词导出", CallbackData: callbackExportByKeyword}},
			{{Text: "📋 导出全部数据", CallbackData: callbackExportAll}},
			{{Text: "🔙 返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) exportFormatKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "👤 仅用户名 (TXT)", CallbackData: callbackExportFormatUsers}},
			{{Text: "🆔 仅用户ID (TXT)", CallbackData: callbackExportFormatIDs}},
			{{Text: "📊 完整记录 (CSV)", CallbackData: callbackExportFormatCSV}},
			{{Text: "🔙 返回", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) cancelExportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "🔙 取消", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) legacyRulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "开关监控", CallbackData: callbackToggleMonitor}, {Text: "切换 Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "设置冷却", CallbackData: callbackSetCooldown}, {Text: "设置模板", CallbackData: callbackSetTemplate}},
			{{Text: "最大消息长度", CallbackData: callbackSetMaxLength}, {Text: "最小账号年龄", CallbackData: callbackSetMinAge}},
			{{Text: "过滤无用户名", CallbackData: callbackToggleNoName}, {Text: "过滤无头像", CallbackData: callbackToggleNoPhoto}},
			{{Text: "监听群配置", CallbackData: callbackChats}, {Text: "黑名单", CallbackData: callbackBlacklist}},
			{{Text: "当前聊天设为通知群", CallbackData: callbackSetAlertChat}},
			{{Text: "🔙 返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) rulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "开关监控", CallbackData: callbackToggleMonitor}, {Text: "切换 Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "同用户冷却", CallbackData: callbackSetCooldown}, {Text: "同群冷却", CallbackData: callbackSetChatCooldown}},
			{{Text: "同内容冷却", CallbackData: callbackSetTextCooldown}, {Text: "设置模板", CallbackData: callbackSetTemplate}},
			{{Text: "最小消息长度", CallbackData: callbackSetMinLength}, {Text: "最大消息长度", CallbackData: callbackSetMaxLength}},
			{{Text: "最小账号年龄", CallbackData: callbackSetMinAge}, {Text: "过滤无用户名", CallbackData: callbackToggleNoName}},
			{{Text: "过滤无头像", CallbackData: callbackToggleNoPhoto}},
			{{Text: "监听群配置", CallbackData: callbackChats}, {Text: "黑名单", CallbackData: callbackBlacklist}},
			{{Text: "当前聊天设为通知群", CallbackData: callbackSetAlertChat}},
			{{Text: "🔙 返回主菜单", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) chatsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "➕ 添加监听群", CallbackData: callbackAddChat}, {Text: "🗑 移除监听群", CallbackData: callbackRemoveChat}},
			{{Text: "🔙 返回", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) blacklistKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "移出黑名单用户", CallbackData: callbackUnblockUser}, {Text: "移出黑名单群", CallbackData: callbackUnblockChat}},
			{{Text: "🔙 返回", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) statusKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "🔙 返回主菜单", CallbackData: callbackMain}},
		},
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

func (s *AdminService) sendExportFile(ctx context.Context, chatID, userID int64, format string) error {
	exportCtx, ok := s.getExportContext(userID)
	if !ok {
		return fmt.Errorf("请先选择导出条件")
	}

	records := s.filterMatchRecords(exportCtx)
	if len(records) == 0 {
		return fmt.Errorf("没有匹配到可导出的数据")
	}

	var (
		filename string
		data     []byte
		err      error
	)

	switch format {
	case callbackExportFormatUsers:
		filename = fmt.Sprintf("jtbot_users_%s.txt", time.Now().Format("20060102_150405"))
		data = []byte(strings.Join(uniqueUsernames(records), "\n"))
	case callbackExportFormatIDs:
		filename = fmt.Sprintf("jtbot_userids_%s.txt", time.Now().Format("20060102_150405"))
		data = []byte(strings.Join(uniqueUserIDs(records), "\n"))
	case callbackExportFormatCSV:
		filename = fmt.Sprintf("jtbot_records_%s.csv", time.Now().Format("20060102_150405"))
		data, err = buildCSV(records)
	default:
		return fmt.Errorf("未知导出格式")
	}
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("导出结果为空")
	}

	caption := fmt.Sprintf("导出完成，共 %d 条记录。", len(records))
	if err := s.client.SendDocument(ctx, chatID, filename, data, caption); err != nil {
		return err
	}
	s.clearExportContext(userID)
	return nil
}

func (s *AdminService) filterMatchRecords(exportCtx exportContext) []model.MatchRecord {
	records := s.recordStore.MatchRecords()
	result := make([]model.MatchRecord, 0, len(records))
	keywordSet := make(map[string]struct{}, len(exportCtx.keywords))
	for _, keyword := range exportCtx.keywords {
		keywordSet[strings.ToLower(strings.TrimSpace(keyword))] = struct{}{}
	}

	for _, record := range records {
		switch exportCtx.filterType {
		case exportByTime:
			if record.MatchedAt.Before(exportCtx.start) || record.MatchedAt.After(exportCtx.end) {
				continue
			}
		case exportByWords:
			if _, ok := keywordSet[strings.ToLower(strings.TrimSpace(record.Keyword))]; !ok {
				continue
			}
		}
		result = append(result, record)
	}
	return result
}

func buildCSV(records []model.MatchRecord) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write([]string{"用户ID", "用户名", "昵称", "来源群组", "触发关键词", "触发时间", "消息内容", "监控账号"}); err != nil {
		return nil, err
	}
	for _, record := range records {
		if err := writer.Write([]string{
			strconv.FormatInt(record.UserID, 10),
			record.Username,
			record.Name,
			record.ChatTitle,
			record.Keyword,
			record.MatchedAt.Format("2006-01-02 15:04:05"),
			record.Message,
			record.Monitor,
		}); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return buf.Bytes(), writer.Error()
}

func uniqueUsernames(records []model.MatchRecord) []string {
	set := map[string]struct{}{}
	for _, record := range records {
		username := strings.TrimSpace(record.Username)
		if username == "" {
			continue
		}
		if !strings.HasPrefix(username, "@") {
			username = "@" + username
		}
		set[username] = struct{}{}
	}
	return sortedKeys(set)
}

func uniqueUserIDs(records []model.MatchRecord) []string {
	set := map[string]struct{}{}
	for _, record := range records {
		set[strconv.FormatInt(record.UserID, 10)] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]struct{}) []string {
	items := make([]string, 0, len(set))
	for item := range set {
		items = append(items, item)
	}
	sort.Strings(items)
	return items
}

func parseTimeRange(text string) (time.Time, time.Time, error) {
	text = strings.TrimSpace(text)
	parts := strings.Split(text, "|")
	if len(parts) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid range")
	}

	start, err := parseFlexibleTime(parts[0])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := parseFlexibleTime(parts[1])
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("end must be after start")
	}
	return start, end, nil
}

func parseFlexibleTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	now := time.Now()
	location := now.Location()
	layouts := []string{
		"2006-01-02 15:04",
		"2006-01-02-15:04",
		"01-02 15:04",
		"01-02-15:04",
	}
	for _, layout := range layouts {
		if strings.HasPrefix(layout, "2006") {
			if parsed, err := time.ParseInLocation(layout, raw, location); err == nil {
				return parsed, nil
			}
			continue
		}

		withYear := fmt.Sprintf("%d-%s", now.Year(), raw)
		yearLayout := "2006-" + layout
		if parsed, err := time.ParseInLocation(yearLayout, withYear, location); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format")
}

func filepathBase(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "-"
	}
	path = strings.ReplaceAll(path, "\\", "/")
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
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

func splitTemplateParts(text string) []string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	parts := strings.Split(text, "---")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	if len(result) > 0 {
		return result
	}
	return splitInputParts(text)
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

func dryRunLabel(enabled bool) string {
	if enabled {
		return "演练模式（不真实私信）"
	}
	return "真实发送"
}

func (s *AdminService) editMessageText(ctx context.Context, chatID int64, messageID int64, text string, keyboard *model.InlineKeyboardMarkup) error {
	text = trimTelegramText(text, 3500)
	err := s.client.EditMessageText(ctx, chatID, messageID, text, keyboard)
	if err == nil {
		return nil
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "message is not modified"):
		return nil
	case strings.Contains(lower, "message_too_long"), strings.Contains(lower, "message is too long"):
		return s.client.EditMessageText(ctx, chatID, messageID, trimTelegramText(text, 3000), keyboard)
	default:
		return err
	}
}

func trimTelegramText(text string, limit int) string {
	runes := []rune(strings.TrimSpace(text))
	if len(runes) <= limit || limit <= 0 {
		return string(runes)
	}
	if limit <= 20 {
		return string(runes[:limit])
	}
	return string(runes[:limit-12]) + "\n\n…… 内容过长，已截断"
}

func (s *AdminService) listMonitorAccounts() []MonitorAccountInfo {
	if s.monitorManager == nil {
		return nil
	}
	return s.monitorManager.ListMonitorAccounts()
}

func (s *AdminService) listDMAccounts() []DMAccountInfo {
	if s.dmManager == nil {
		return nil
	}
	return s.dmManager.ListDMAccounts()
}

func (s *AdminService) dmStatsToday() (sent, success, failed int) {
	today := time.Now().Format("2006-01-02")
	for _, record := range s.recordStore.DMRecords() {
		if record.SentAt.Format("2006-01-02") != today {
			continue
		}
		sent++
		switch record.Status {
		case "sent", "success":
			success++
		case "failed":
			failed++
		}
	}
	return sent, success, failed
}

func (s *AdminService) handleDMCheckAll(ctx context.Context, callback *model.CallbackQuery) error {
	if s.dmManager == nil {
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "当前没有可用的私信号管理器")
	}

	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "当前没有私信号可检查")
	}

	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "开始批量检查私信号状态"); err != nil {
		return err
	}

	counts := newDMStatusCounts()
	if err := s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, s.dmCheckProgressText(0, len(accounts), counts), nil); err != nil {
		return err
	}

	for i, account := range accounts {
		code := "failed"
		if result, err := s.dmManager.CheckDMAccount(ctx, account.Phone); err == nil {
			code = normalizeDMStatusCode(result.StatusCode)
		}
		counts[code]++

		if i == len(accounts)-1 || (i+1)%3 == 0 {
			if err := s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, s.dmCheckProgressText(i+1, len(accounts), counts), nil); err != nil {
				return err
			}
		}
	}

	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, s.dmCheckResultText(len(accounts), counts), s.dmCheckActionsKeyboard())
}

func (s *AdminService) handleDMExportAbnormal(ctx context.Context, callback *model.CallbackQuery) error {
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "开始导出异常私信号"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "✅ 当前没有异常私信号，所有账号都属于正常无限制状态。", s.dmCheckActionsKeyboard())
	}

	if err := s.sendDMAccountExportDocuments(ctx, callback.Message.Chat.ID, accounts, "异常私信号"); err != nil {
		return err
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("📤 异常账号已导出\n\n已导出: %d 个\n已删除: %d 个\n删除失败: %d 个\n\n已保留: 仅正常无限制账号", len(accounts), removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) handleDMKeepOnlyNormal(ctx context.Context, callback *model.CallbackQuery) error {
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "开始清理异常私信号"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "✅ 当前没有异常私信号，已经只保留正常无限制账号。", s.dmAccountsKeyboard())
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("🧹 私信号池已清理\n\n已删除异常账号: %d 个\n删除失败: %d 个\n当前只保留正常无限制账号。", removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) dmCheckProgressText(done, total int, counts map[string]int) string {
	return fmt.Sprintf(
		"🔍 正在检查私信号状态 (%d/%d)\n\n%s",
		done,
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckResultText(total int, counts map[string]int) string {
	return fmt.Sprintf(
		"✅ 私信号状态检查完成\n\n总计: %d 个账号\n%s\n⚠️ 接下来你可以导出异常账号，或者直接只保留正常无限制账号。",
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckActionsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "📤 导出异常并删除", CallbackData: callbackDMExportAbnormal},
				{Text: "🧹 仅保留正常账号", CallbackData: callbackDMKeepOnlyNormal},
			},
			{
				{Text: "📋 返回账号列表", CallbackData: callbackDMList},
				{Text: "🔙 返回私信号池", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) collectDMAccountsByStatus(keepActive bool) []DMAccountInfo {
	accounts := s.listDMAccounts()
	filtered := make([]DMAccountInfo, 0, len(accounts))
	for _, account := range accounts {
		code := effectiveDMStatusCode(account)
		if keepActive {
			if code == "active" {
				filtered = append(filtered, account)
			}
			continue
		}
		if code != "active" {
			filtered = append(filtered, account)
		}
	}
	return filtered
}

func (s *AdminService) deleteDMAccounts(ctx context.Context, accounts []DMAccountInfo) (removed int, failed int) {
	for _, account := range accounts {
		if err := s.dmManager.DeleteDMAccount(ctx, account.Phone); err != nil {
			failed++
			continue
		}
		removed++
	}
	return removed, failed
}

func (s *AdminService) sendDMAccountExportDocuments(ctx context.Context, chatID int64, accounts []DMAccountInfo, label string) error {
	timestamp := time.Now().Format("20060102_150405")

	zipData, sessionCount, err := buildDMAccountSessionsZip(accounts)
	if err != nil {
		return err
	}
	if sessionCount > 0 {
		filename := fmt.Sprintf("dm_abnormal_sessions_%s.zip", timestamp)
		caption := fmt.Sprintf("📦 %s Session 打包（%d 个）", label, sessionCount)
		if err := s.client.SendDocument(ctx, chatID, filename, zipData, caption); err != nil {
			return err
		}
	}

	reportData := buildDMAccountReport(accounts, label)
	reportName := fmt.Sprintf("dm_abnormal_accounts_%s.txt", timestamp)
	reportCaption := fmt.Sprintf("📋 %s列表（%d 个）", label, len(accounts))
	return s.client.SendDocument(ctx, chatID, reportName, reportData, reportCaption)
}

func buildDMAccountSessionsZip(accounts []DMAccountInfo) ([]byte, int, error) {
	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)
	count := 0

	for _, account := range accounts {
		path := strings.TrimSpace(account.SessionFile)
		if path == "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		name := filepathBase(path)
		entry, err := writer.Create(name)
		if err != nil {
			_ = writer.Close()
			return nil, count, err
		}
		if _, err := entry.Write(data); err != nil {
			_ = writer.Close()
			return nil, count, err
		}
		count++
	}

	if err := writer.Close(); err != nil {
		return nil, count, err
	}
	return buf.Bytes(), count, nil
}

func buildDMAccountReport(accounts []DMAccountInfo, label string) []byte {
	lines := []string{
		fmt.Sprintf("# %s", label),
		fmt.Sprintf("# 导出时间: %s", time.Now().Format("2006-01-02 15:04:05")),
		fmt.Sprintf("# 共 %d 个账号", len(accounts)),
		"",
	}

	for _, account := range accounts {
		code := effectiveDMStatusCode(account)
		line := fmt.Sprintf(
			"%s | %s | 今日发送 %d 条 | 今日成功 %d 条 | 今日失败 %d 条",
			account.Phone,
			dmStatusLabel(code),
			account.TodaySent,
			account.TodaySuccess,
			account.TodayFailed,
		)
		if summary := strings.TrimSpace(account.StatusSummary); summary != "" {
			line += " | " + summary
		}
		lines = append(lines, line)
	}

	return []byte(strings.Join(lines, "\n"))
}

func newDMStatusCounts() map[string]int {
	return map[string]int{
		"active":     0,
		"restricted": 0,
		"spam":       0,
		"banned":     0,
		"frozen":     0,
		"failed":     0,
		"unknown":    0,
	}
}

func formatDMStatusCounts(counts map[string]int) string {
	lines := []string{
		fmt.Sprintf("✅ 正常无限制: %d", counts["active"]),
		fmt.Sprintf("⚠️ 临时限制 / 双向限制: %d", counts["restricted"]),
		fmt.Sprintf("📵 垃圾消息风控: %d", counts["spam"]),
		fmt.Sprintf("🚫 封禁账号: %d", counts["banned"]),
		fmt.Sprintf("❄️ 冻结 / 审核中: %d", counts["frozen"]),
		fmt.Sprintf("🔌 离线 / 检查失败: %d", counts["failed"]),
	}
	if counts["unknown"] > 0 {
		lines = append(lines, fmt.Sprintf("❓ 未识别状态: %d", counts["unknown"]))
	}
	return strings.Join(lines, "\n")
}

func effectiveDMStatusCode(account DMAccountInfo) string {
	code := normalizeDMStatusCode(account.StatusCode)
	if code != "unknown" || strings.TrimSpace(account.StatusSummary) == "" {
		return code
	}

	summary := strings.ToLower(strings.TrimSpace(account.StatusSummary))
	switch {
	case strings.Contains(summary, "正常"), strings.Contains(summary, "无限制"):
		return "active"
	case strings.Contains(summary, "双向"), strings.Contains(summary, "临时限制"):
		return "restricted"
	case strings.Contains(summary, "风控"), strings.Contains(summary, "垃圾消息"):
		return "spam"
	case strings.Contains(summary, "封禁"), strings.Contains(summary, "永久限制"):
		return "banned"
	case strings.Contains(summary, "审核"), strings.Contains(summary, "验证"), strings.Contains(summary, "冻结"):
		return "frozen"
	case strings.Contains(summary, "失败"), strings.Contains(summary, "离线"):
		return "failed"
	default:
		return "unknown"
	}
}

func normalizeDMStatusCode(code string) string {
	switch strings.TrimSpace(strings.ToLower(code)) {
	case "active", "restricted", "spam", "banned", "frozen", "failed":
		return strings.TrimSpace(strings.ToLower(code))
	default:
		return "unknown"
	}
}

func dmStatusLabel(code string) string {
	switch normalizeDMStatusCode(code) {
	case "active":
		return "✅ 正常无限制"
	case "restricted":
		return "⚠️ 临时限制 / 双向限制"
	case "spam":
		return "📵 垃圾消息风控"
	case "banned":
		return "🚫 封禁"
	case "frozen":
		return "❄️ 冻结 / 审核中"
	case "failed":
		return "🔌 离线 / 检查失败"
	default:
		return "❓ 未识别"
	}
}

func (s *AdminService) sendFailedDMExport(ctx context.Context, chatID int64) error {
	records := s.recordStore.DMRecords()
	failed := make([]model.DMRecord, 0)
	for _, record := range records {
		if record.Status == "failed" {
			failed = append(failed, record)
		}
	}
	if len(failed) == 0 {
		return fmt.Errorf("没有异常私信记录")
	}

	lines := make([]string, 0, len(failed))
	for _, record := range failed {
		target := strconv.FormatInt(record.UserID, 10)
		if strings.TrimSpace(record.Username) != "" {
			target = "@" + strings.TrimPrefix(record.Username, "@")
		}
		line := target
		if strings.TrimSpace(record.Error) != "" {
			line += " | " + record.Error
		}
		lines = append(lines, line)
	}

	filename := fmt.Sprintf("jtbot_failed_dm_%s.txt", time.Now().Format("20060102_150405"))
	return s.client.SendDocument(ctx, chatID, filename, []byte(strings.Join(lines, "\n")), fmt.Sprintf("异常私信记录 %d 条", len(failed)))
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

func (s *AdminService) setExportContext(userID int64, ctx exportContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exportCtx[userID] = ctx
}

func (s *AdminService) getExportContext(userID int64) (exportContext, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, ok := s.exportCtx[userID]
	return ctx, ok
}

func (s *AdminService) clearExportContext(userID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.exportCtx, userID)
}

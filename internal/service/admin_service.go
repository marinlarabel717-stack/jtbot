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
	"github.com/marinlarabel717-stack/jtbot/internal/version"
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
	callbackDMTemplateAddText   = "admin:dm:template:add:text"
	callbackDMTemplateAddPost   = "admin:dm:template:add:postbot"
	callbackDMTemplateAddFwd    = "admin:dm:template:add:forward"
	callbackDMTemplateAddHide   = "admin:dm:template:add:hidden_forward"
	callbackDMTemplateAddQuick  = "admin:dm:template:add:quick_reply"
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
	pendingAddDMText        pendingAction = "add_dm_text"
	pendingAddDMPostBot     pendingAction = "add_dm_postbot"
	pendingAddDMForward     pendingAction = "add_dm_forward"
	pendingAddDMHidden      pendingAction = "add_dm_hidden"
	pendingAddDMQuickReply  pendingAction = "add_dm_quick_reply"
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
			return true, s.client.SendMessage(ctx, msg.Chat.ID, "è¯·å‘é€ `.session` æˆ– `.zip` æ–‡ä»¶ã€‚", s.dmPoolKeyboard())
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
		return true, s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å½“å‰èŠå¤© ID: %d", msg.Chat.ID), nil)
	}

	if s.authInput != nil && !strings.HasPrefix(text, "/") {
		if handled, kind := s.authInput.Submit(text); handled {
			ack := "å·²æ”¶åˆ°ç™»å½•éªŒè¯ç ï¼Œæ­£åœ¨ç»§ç»­ç™»å½•ã€‚"
			if kind == "password" {
				ack = "å·²æ”¶åˆ°ä¸¤æ­¥éªŒè¯å¯†ç ï¼Œæ­£åœ¨ç»§ç»­ç™»å½•ã€‚"
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
		return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "æ²¡æœ‰æ¶ˆæ¯ä¸Šä¸‹æ–‡")
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
		alert = "æŠŠæ‰‹æœºå·ç›´æŽ¥å‘ç»™æˆ‘"
	case callbackAccountList:
		text, keyboard = s.accountsListText(), s.accountsListKeyboard()
	case callbackKeywords:
		text, keyboard = s.keywordsText(), s.keywordsKeyboard()
	case callbackKeywordAdd:
		s.setPending(callback.From.ID, pendingAddKeywordsFuzzy)
		text, keyboard = "å‘é€è¦æ·»åŠ çš„æ¨¡ç³Šå…³é”®è¯ï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚\n\næ¨¡ç³Šå…³é”®è¯ï¼šä¸€å¥è¯é‡Œåªè¦åŒ…å«å…³é”®è¯å°±å‘½ä¸­ã€‚", s.keywordsKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€æ¨¡ç³Šå…³é”®è¯"
	case callbackKeywordAdd + ":exact":
		s.setPending(callback.From.ID, pendingAddKeywordsExact)
		text, keyboard = "å‘é€è¦æ·»åŠ çš„ç²¾å‡†å…³é”®è¯ï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚\n\nç²¾å‡†å…³é”®è¯ï¼šæ¶ˆæ¯å†…å®¹å¿…é¡»ä¸Žå…³é”®è¯å®Œå…¨ä¸€è‡´æ‰å‘½ä¸­ã€‚", s.keywordsKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€ç²¾å‡†å…³é”®è¯"
	case callbackKeywordRemove:
		s.setPending(callback.From.ID, pendingRemoveKeyword)
		text, keyboard = "å‘é€è¦åˆ é™¤çš„å…³é”®è¯ï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚\n\næ”¯æŒï¼š\nç²¾å‡†:å…³é”®è¯\næ¨¡ç³Š:å…³é”®è¯\næˆ–ç›´æŽ¥å‘å…³é”®è¯æ–‡æœ¬ï¼ˆä¼šåˆ é™¤åŒåçš„ç²¾å‡†/æ¨¡ç³Šè§„åˆ™ï¼‰ã€‚", s.keywordsKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€è¦åˆ é™¤çš„å…³é”®è¯"
	case callbackDMPool:
		text, keyboard = s.dmPoolText(), s.dmPoolKeyboard()
	case callbackDMConnect:
		s.setPending(callback.From.ID, pendingLoginDM)
		text, keyboard = s.dmConnectPrompt(), s.dmPoolKeyboard()
		alert = "æŠŠç§ä¿¡å·æ‰‹æœºå·ç›´æŽ¥å‘ç»™æˆ‘"
	case callbackDMUpload:
		s.setPending(callback.From.ID, pendingUploadDMSess)
		text, keyboard = s.dmUploadPrompt(), s.dmPoolKeyboard()
		alert = "æŠŠ session æ–‡ä»¶ç›´æŽ¥å‘ç»™æˆ‘"
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
		s.setPending(callback.From.ID, pendingNone)
		text, keyboard = "选择要添加的话术发送模式。", s.dmTemplateModeKeyboard()
		alert = "请选择话术发送模式"
	case callbackDMTemplateAddText:
		s.setPending(callback.From.ID, pendingAddDMText)
		text, keyboard = s.dmTemplateTextPrompt(), s.dmTemplateModeKeyboard()
		alert = "发送文本话术内容"
	case callbackDMTemplateAddPost:
		s.setPending(callback.From.ID, pendingAddDMPostBot)
		text, keyboard = s.dmTemplatePostBotPrompt(), s.dmTemplateModeKeyboard()
		alert = "发送 PostBot 代码"
	case callbackDMTemplateAddFwd:
		s.setPending(callback.From.ID, pendingAddDMForward)
		text, keyboard = s.dmTemplateForwardPrompt(false), s.dmTemplateModeKeyboard()
		alert = "发送频道贴文链接"
	case callbackDMTemplateAddHide:
		s.setPending(callback.From.ID, pendingAddDMHidden)
		text, keyboard = s.dmTemplateForwardPrompt(true), s.dmTemplateModeKeyboard()
		alert = "发送隐藏来源转发链接"
	case callbackDMTemplateAddQuick:
		s.setPending(callback.From.ID, pendingAddDMQuickReply)
		text, keyboard = s.dmTemplateQuickReplyPrompt(), s.dmTemplateModeKeyboard()
		alert = "发送快捷回复 ID"
	case callbackDMTemplateRemove:
		s.setPending(callback.From.ID, pendingRemoveDMTpl)
		text, keyboard = "发送要删除的话术编号或内容，支持一次删除多条，使用 |、换行或逗号分隔。", s.dmTemplatesKeyboard()
		alert = "等待你发送要删除的话术"
	case callbackDMRecords:
		text, keyboard = s.dmRecordsText(), s.dmRecordsKeyboard()
	case callbackDMExportFailed:
		err = s.sendFailedDMExport(ctx, callback.Message.Chat.ID)
		alert = "å¼‚å¸¸ç§ä¿¡è®°å½•å·²å¯¼å‡º"
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
		text, keyboard = "å·²é€‰æ‹©å¯¼å‡ºå…¨éƒ¨æ•°æ®ï¼Œè¯·é€‰æ‹©å¯¼å‡ºæ ¼å¼ã€‚", s.exportFormatKeyboard()
	case callbackExportFormatUsers, callbackExportFormatIDs, callbackExportFormatCSV:
		err = s.sendExportFile(ctx, callback.Message.Chat.ID, callback.From.ID, callback.Data)
		alert = "å¯¼å‡ºæ–‡ä»¶å·²å‘é€"
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
				alert = "ç›‘æŽ§å·²å¼€å¯"
			} else {
				alert = "ç›‘æŽ§å·²å…³é—­"
			}
		}
	case callbackToggleDryRun:
		var enabled bool
		enabled, err = s.settings.ToggleDryRun()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "å·²åˆ‡åˆ° dry-run"
			} else {
				alert = "å·²åˆ‡åˆ°çœŸå®žå‘é€"
			}
		}
	case callbackSetCooldown:
		s.setPending(callback.From.ID, pendingSetCooldown)
		text, keyboard = "å‘é€æ–°çš„åŒç”¨æˆ·é‡å¤ç§ä¿¡å†·å´åˆ†é’Ÿæ•°ï¼Œæ¯”å¦‚ 1440ã€‚", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€å†·å´åˆ†é’Ÿæ•°"
	case callbackSetChatCooldown:
		s.setPending(callback.From.ID, pendingSetChatCooldown)
		text, keyboard = "å‘é€åŒç¾¤é‡å¤ç§ä¿¡å†·å´åˆ†é’Ÿæ•°ï¼Œå¡« 0 è¡¨ç¤ºå…³é—­ã€‚", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€åŒç¾¤å†·å´åˆ†é’Ÿæ•°"
	case callbackSetTextCooldown:
		s.setPending(callback.From.ID, pendingSetTextCooldown)
		text, keyboard = "å‘é€åŒå†…å®¹é‡å¤ç§ä¿¡å†·å´åˆ†é’Ÿæ•°ï¼Œå¡« 0 è¡¨ç¤ºå…³é—­ã€‚", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€åŒå†…å®¹å†·å´åˆ†é’Ÿæ•°"
	case callbackSetTemplate:
		s.setPending(callback.From.ID, pendingSetTemplate)
		text, keyboard = "å‘é€æ–°çš„ç§ä¿¡æ¨¡æ¿ã€‚å¯ç”¨å˜é‡: {username} {chat_title} {keywords} {message}", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€ç§ä¿¡æ¨¡æ¿"
	case callbackSetMinLength:
		s.setPending(callback.From.ID, pendingSetMinLength)
		text, keyboard = "å‘é€æœ€å°æ¶ˆæ¯é•¿åº¦ï¼Œå¡« 0 è¡¨ç¤ºä¸é™åˆ¶ã€‚", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€æœ€å°æ¶ˆæ¯é•¿åº¦"
	case callbackSetMaxLength:
		s.setPending(callback.From.ID, pendingSetMaxLength)
		text, keyboard = "å‘é€æœ€å¤§æ¶ˆæ¯é•¿åº¦ï¼Œå¡« 0 è¡¨ç¤ºä¸é™åˆ¶ã€‚", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€æœ€å¤§æ¶ˆæ¯é•¿åº¦"
	case callbackSetMinAge:
		s.setPending(callback.From.ID, pendingSetMinAge)
		text, keyboard = "å‘é€æœ€å°è´¦å·å¹´é¾„å¤©æ•°ï¼Œå¡« 0 è¡¨ç¤ºä¸é™åˆ¶ã€‚", s.rulesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€è´¦å·å¹´é¾„"
	case callbackToggleNoName:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoUsername()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "å·²å¼€å¯æ— ç”¨æˆ·åè¿‡æ»¤"
			} else {
				alert = "å·²å…³é—­æ— ç”¨æˆ·åè¿‡æ»¤"
			}
		}
	case callbackToggleNoPhoto:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoAvatar()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "å·²å¼€å¯æ— å¤´åƒè¿‡æ»¤"
			} else {
				alert = "å·²å…³é—­æ— å¤´åƒè¿‡æ»¤"
			}
		}
	case callbackChats:
		text, keyboard = s.chatsText(), s.chatsKeyboard()
	case callbackAddChat:
		s.setPending(callback.From.ID, pendingAddChatIDs)
		text, keyboard = "å‘é€è¦æ·»åŠ çš„ç›‘å¬ç¾¤ IDï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚", s.chatsKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€ç¾¤ ID"
	case callbackRemoveChat:
		s.setPending(callback.From.ID, pendingRemoveChatIDs)
		text, keyboard = "å‘é€è¦ç§»é™¤çš„ç›‘å¬ç¾¤ IDï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚", s.chatsKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€è¦ç§»é™¤çš„ç¾¤ ID"
	case callbackSetAlertChat:
		err = s.settings.SetAlertChatID(callback.Message.Chat.ID)
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			alert = "å½“å‰èŠå¤©å·²è®¾ä¸ºé€šçŸ¥ç¾¤"
		}
	case callbackBlacklist:
		text, keyboard = s.blacklistText(), s.blacklistKeyboard()
	case callbackUnblockUser:
		s.setPending(callback.From.ID, pendingUnblockUsers)
		text, keyboard = "å‘é€è¦ç§»å‡ºé»‘åå•çš„ç”¨æˆ· IDï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚", s.blacklistKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€ç”¨æˆ· ID"
	case callbackUnblockChat:
		s.setPending(callback.From.ID, pendingUnblockChats)
		text, keyboard = "å‘é€è¦ç§»å‡ºé»‘åå•çš„ç¾¤ IDï¼Œå¤šä¸ªç”¨ | æˆ–æ¢è¡Œåˆ†éš”ã€‚", s.blacklistKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€ç¾¤ ID"
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
				alert = "å·²é‡æ–°å‘èµ·è¿žæŽ¥"
				text, keyboard, _ = s.accountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackAccountDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackAccountDeletePrefix)
			err = s.monitorManager.DeleteMonitor(ctx, phone)
			if err == nil {
				alert = "ç›‘æŽ§å·å·²åˆ é™¤"
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
				alert = "å·²é‡æ–°è¿žæŽ¥ç§ä¿¡å·"
				text, keyboard, _ = s.dmAccountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackDMDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMDeletePrefix)
			err = s.dmManager.DeleteDMAccount(ctx, phone)
			if err == nil {
				alert = "ç§ä¿¡å·å·²åˆ é™¤"
				text, keyboard = s.dmAccountsText(), s.dmAccountsKeyboard()
			}
		case strings.HasPrefix(callback.Data, callbackBlockUser):
			alert, err = s.blockUserCallback(callback.Data)
		case strings.HasPrefix(callback.Data, callbackBlockChat):
			alert, err = s.blockChatCallback(callback.Data)
		default:
			return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "æœªçŸ¥æ“ä½œ")
		}
	}

	if err == nil && text != "" {
		err = s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, keyboard)
	}

	answerText := alert
	if err != nil {
		answerText = "æ“ä½œå¤±è´¥: " + err.Error()
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
			return s.client.SendMessage(ctx, msg.Chat.ID, "æ‰‹æœºå·ä¸èƒ½ä¸ºç©ºã€‚", s.backToAccountsKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ä¸€æ¬¡å…ˆç™»å½•ä¸€ä¸ªç›‘æŽ§å·ã€‚å¤šä¸ªç›‘æŽ§å·è¯·é€ä¸ªæ·»åŠ ã€‚", s.backToAccountsKeyboard())
		}
		if s.monitorManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "å½“å‰æ²¡æœ‰å¯ç”¨çš„ç›‘æŽ§è´¦å·ç®¡ç†å™¨ã€‚", s.backToAccountsKeyboard())
		}
		result, err := s.monitorManager.StartMonitorLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "å¯åŠ¨ç™»å½•å¤±è´¥: "+err.Error(), s.backToAccountsKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.accountsOverviewText(), s.accountsMenuKeyboard())
	case pendingLoginDM:
		phones := splitInputParts(text)
		if len(phones) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æ‰‹æœºå·ä¸èƒ½ä¸ºç©ºã€‚", s.dmPoolKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ä¸€æ¬¡å…ˆç™»å½•ä¸€ä¸ªç§ä¿¡å·ã€‚å¤šä¸ªç§ä¿¡å·è¯·é€ä¸ªæ·»åŠ ã€‚", s.dmPoolKeyboard())
		}
		if s.dmManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "å½“å‰æ²¡æœ‰å¯ç”¨çš„ç§ä¿¡å·ç®¡ç†å™¨ã€‚", s.dmPoolKeyboard())
		}
		result, err := s.dmManager.StartDMLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "å¯åŠ¨ç§ä¿¡å·ç™»å½•å¤±è´¥: "+err.Error(), s.dmPoolKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.dmPoolText(), s.dmPoolKeyboard())
	case pendingAddKeywordsExact:
		added, err := s.keywordStore.AddWithMode("exact", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²æ·»åŠ  %d ä¸ªç²¾å‡†å…³é”®è¯ã€‚\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingAddKeywordsFuzzy, pendingAddKeywords:
		added, err := s.keywordStore.AddWithMode("fuzzy", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²æ·»åŠ  %d ä¸ªæ¨¡ç³Šå…³é”®è¯ã€‚\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingRemoveKeyword:
		removed, err := s.keywordStore.Remove(splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²åˆ é™¤ %d ä¸ªå…³é”®è¯ã€‚\n\n%s", removed, s.keywordsText()), s.keywordsKeyboard())
	case pendingSetCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "åŒç”¨æˆ·é‡å¤ç§ä¿¡å†·å´å¿…é¡»æ˜¯æ­£æ•´æ•°ã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "åŒç”¨æˆ·é‡å¤ç§ä¿¡å†·å´å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetChatCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "åŒç¾¤é‡å¤ç§ä¿¡å†·å´å¿…é¡»æ˜¯éžè´Ÿæ•´æ•°ã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetChatCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "åŒç¾¤é‡å¤ç§ä¿¡å†·å´å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTextCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "åŒå†…å®¹é‡å¤ç§ä¿¡å†·å´å¿…é¡»æ˜¯éžè´Ÿæ•´æ•°ã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetTextCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "åŒå†…å®¹é‡å¤ç§ä¿¡å†·å´å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTemplate:
		if strings.TrimSpace(text) == "" {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æ¨¡æ¿ä¸èƒ½ä¸ºç©ºã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetDMTemplate(text); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ç§ä¿¡æ¨¡æ¿å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddDMTemplate:
		templates, parseErr := parseDMTemplateInputs(text)
		if parseErr != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, parseErr.Error()+"\n\n"+s.dmTemplateAddPrompt(), s.dmTemplatesKeyboard())
		}
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
	case pendingAddDMText:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeTextDMTemplate(text), "文本直发")
	case pendingAddDMPostBot:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodePostBotDMTemplate(text), "内联Bot @PostBot")
	case pendingAddDMForward:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeForwardDMTemplate(text), "频道贴文转发")
	case pendingAddDMHidden:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeHiddenForwardDMTemplate(text), "隐藏转发来源")
	case pendingAddDMQuickReply:
		shortcutID, err := parseNonNegativeInt(text)
		if err != nil || shortcutID <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "企业快捷回复 ID 必须是大于 0 的数字。", s.dmTemplateModeKeyboard())
		}
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeQuickReplyDMTemplate(shortcutID), "企业快捷回复")
	case pendingRemoveDMTpl:
		removedTargets := resolveDMTemplateRemovals(text, s.settings.ListDMTemplates())
		if len(removedTargets) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "没有识别到可删除的话术编号或内容。", s.dmTemplatesKeyboard())
		}
		removed, err := s.settings.RemoveDMTemplates(removedTargets)
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("已删除 %d 条话术。\n\n%s", removed, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingSetMaxLength:
		maxLength, err := parseNonNegativeInt(text)
		if err != nil || maxLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æœ€å¤§æ¶ˆæ¯é•¿åº¦å¿…é¡»æ˜¯éžè´Ÿæ•´æ•°ã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetMaxMessageLength(maxLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "æœ€å¤§æ¶ˆæ¯é•¿åº¦å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinLength:
		minLength, err := parseNonNegativeInt(text)
		if err != nil || minLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æœ€å°æ¶ˆæ¯é•¿åº¦å¿…é¡»æ˜¯éžè´Ÿæ•´æ•°ã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetMinMessageLength(minLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "æœ€å°æ¶ˆæ¯é•¿åº¦å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinAge:
		days, err := parseNonNegativeInt(text)
		if err != nil || days < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æœ€å°è´¦å·å¹´é¾„å¿…é¡»æ˜¯éžè´Ÿæ•´æ•°ã€‚", s.rulesKeyboard())
		}
		if err := s.settings.SetMinAccountAgeDays(days); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "è´¦å·å¹´é¾„è¿‡æ»¤å·²æ›´æ–°ã€‚\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddChatIDs:
		added, err := s.settings.AddMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²æ·»åŠ  %d ä¸ªç›‘å¬ç¾¤ã€‚\n\n%s", added, s.chatsText()), s.chatsKeyboard())
	case pendingRemoveChatIDs:
		removed, err := s.settings.RemoveMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²ç§»é™¤ %d ä¸ªç›‘å¬ç¾¤ã€‚\n\n%s", removed, s.chatsText()), s.chatsKeyboard())
	case pendingUnblockUsers:
		removed, err := s.removeBlockedUsers(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²ç§»å‡º %d ä¸ªé»‘åå•ç”¨æˆ·ã€‚\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingUnblockChats:
		removed, err := s.removeBlockedChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²ç§»å‡º %d ä¸ªé»‘åå•ç¾¤ã€‚\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingExportTime:
		start, end, err := parseTimeRange(text)
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æ—¶é—´æ ¼å¼ä¸å¯¹ã€‚\nç¤ºä¾‹: 09-28-12:00 | 09-28-18:30", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByTime,
			start:      start,
			end:        end,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf(
			"å·²é€‰æ‹©æ—¶é—´æ®µï¼š\n%s ~ %s\n\nè¯·é€‰æ‹©å¯¼å‡ºæ ¼å¼ã€‚",
			start.Format("01-02 15:04"),
			end.Format("01-02 15:04"),
		), s.exportFormatKeyboard())
	case pendingExportKeyword:
		keywords := splitInputParts(text)
		if len(keywords) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "å…³é”®è¯ä¸èƒ½ä¸ºç©ºã€‚", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByWords,
			keywords:   keywords,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, "å·²é€‰æ‹©å…³é”®è¯ï¼š"+strings.Join(keywords, ", ")+"\n\nè¯·é€‰æ‹©å¯¼å‡ºæ ¼å¼ã€‚", s.exportFormatKeyboard())
	default:
		return nil
	}
}

func (s *AdminService) handlePendingDMUpload(ctx context.Context, msg *model.Message) error {
	if msg.Document == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "è¯·å‘é€ `.session` æˆ– `.zip` æ–‡ä»¶ã€‚", s.dmPoolKeyboard())
	}
	if s.dmManager == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "å½“å‰æ²¡æœ‰å¯ç”¨çš„ç§ä¿¡å·ç®¡ç†å™¨ã€‚", s.dmPoolKeyboard())
	}

	filename := strings.TrimSpace(msg.Document.FileName)
	lowerName := strings.ToLower(filename)
	if !strings.HasSuffix(lowerName, ".session") && !strings.HasSuffix(lowerName, ".zip") {
		return s.client.SendMessage(ctx, msg.Chat.ID, "æ–‡ä»¶æ ¼å¼ä¸æ”¯æŒï¼Œä»…æ”¯æŒ `.session` æˆ– `.zip`ã€‚", s.dmPoolKeyboard())
	}

	downloader, ok := s.client.(fileDownloader)
	if !ok {
		return s.client.SendMessage(ctx, msg.Chat.ID, "å½“å‰ Bot API å®¢æˆ·ç«¯ä¸æ”¯æŒä¸‹è½½æ–‡ä»¶ã€‚", s.dmPoolKeyboard())
	}

	if err := s.client.SendMessage(ctx, msg.Chat.ID, "å¼€å§‹ä¸‹è½½å¹¶å¯¼å…¥ Sessionï¼Œæ•°é‡å¤šæ—¶ä¼šç¨ç­‰ä¸€ä¼šå„¿ã€‚", nil); err != nil {
		return err
	}

	downloadedName, data, err := downloader.DownloadFile(ctx, msg.Document.FileID)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ä¸‹è½½æ–‡ä»¶å¤±è´¥: "+err.Error(), s.dmPoolKeyboard())
	}
	if strings.TrimSpace(filename) == "" {
		filename = downloadedName
	}

	result, err := s.dmManager.ImportDMSessions(ctx, filename, data)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "å¯¼å…¥ Session å¤±è´¥: "+err.Error(), s.dmPoolKeyboard())
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
		"🤖 JTBot 关键词监控机器人\n版本: %s\n\n📱 监控账号: %d在线 / %d离线\n🔑 关键词: %d个\n💬 私信记录: 发送 %d | 成功 %d | 失败 %d",
		version.Current,
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
	return fmt.Sprintf("ðŸ“± ç›‘æŽ§è´¦å·ç®¡ç†\n\nå·²ç™»å½•è´¦å·: %d\nåœ¨çº¿: %d | ç¦»çº¿: %d", total, active, total-active)
}

func (s *AdminService) accountAddPrompt() string {
	return "è¯·è¾“å…¥ç›‘æŽ§è´¦å·çš„æ‰‹æœºå·ã€‚\n\næ”¯æŒæ ¼å¼ï¼š\nâ€¢ +8613800138000\nâ€¢ 8613800138000\nâ€¢ +66955305284"
}

func (s *AdminService) accountsListText() string {
	accounts := s.listMonitorAccounts()
	if len(accounts) == 0 {
		return "âŒ æš‚æ— ç›‘æŽ§è´¦å·\n\nç‚¹å‡»â€œæ·»åŠ æ–°è´¦å·â€å¼€å§‹æ·»åŠ ã€‚"
	}

	lines := []string{fmt.Sprintf("ðŸ“‹ è´¦å·åˆ—è¡¨ (%dä¸ª)ï¼š", len(accounts)), ""}
	for i, account := range accounts {
		status := "ðŸ”´ ç¦»çº¿"
		if account.Online {
			status = "ðŸŸ¢ åœ¨çº¿"
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
		return "", nil, fmt.Errorf("ç›‘æŽ§å·ä¸å­˜åœ¨")
	}

	status := "ðŸ”´ ç¦»çº¿"
	if account.Online {
		status = "ðŸŸ¢ åœ¨çº¿"
	}
	text := fmt.Sprintf("ðŸ“± è´¦å·è¯¦æƒ…\n\næ‰‹æœºå·: %s\nçŠ¶æ€: %s\nSession: %s", account.Phone, status, filepathBase(account.SessionFile))
	if strings.TrimSpace(account.LastError) != "" {
		text += "\né”™è¯¯: " + account.LastError
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ðŸ”„ é‡æ–°è¿žæŽ¥", CallbackData: callbackAccountRetryPrefix + account.Phone},
				{Text: "âŒ åˆ é™¤è´¦å·", CallbackData: callbackAccountDeletePrefix + account.Phone},
			},
			{
				{Text: "ðŸ”™ è¿”å›žåˆ—è¡¨", CallbackData: callbackAccountList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) keywordsText() string {
	keywords := s.keywordStore.ListEntries()
	if len(keywords) == 0 {
		return "ðŸ“ å…³é”®è¯åˆ—è¡¨ä¸ºç©º"
	}
	lines := []string{fmt.Sprintf("ðŸ“ å…³é”®è¯åˆ—è¡¨ (%dä¸ª)ï¼š", len(keywords)), ""}
	const previewLimit = 60
	for i, keyword := range keywords {
		if i >= previewLimit {
			lines = append(lines, fmt.Sprintf("â€¦â€¦ è¿˜æœ‰ %d ä¸ªå…³é”®è¯æœªå±•å¼€æ˜¾ç¤º", len(keywords)-previewLimit))
			break
		}
		modeLabel := "æ¨¡ç³Š"
		if strings.EqualFold(strings.TrimSpace(keyword.Mode), "exact") {
			modeLabel = "ç²¾å‡†"
		}
		lines = append(lines, fmt.Sprintf("%d. [%s] %s", i+1, modeLabel, keyword.Text))
	}
	lines = append(lines, "", "æ¨¡ç³Šï¼šä¸€å¥è¯é‡ŒåŒ…å«å…³é”®è¯å°±å‘½ä¸­", "ç²¾å‡†ï¼šæ•´å¥å†…å®¹å¿…é¡»ä¸Žå…³é”®è¯å®Œå…¨ä¸€è‡´æ‰å‘½ä¸­")
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
		"ðŸ’¬ ç§ä¿¡å·æ± \n\nå·²ç™»å½•è´¦å·: %d\nåœ¨çº¿: %d | ç¦»çº¿: %d\nè¯æœ¯æ¨¡æ¿: %d æ¡\nä»Šæ—¥ç§ä¿¡: å‘é€ %d | æˆåŠŸ %d | å¤±è´¥ %d\n\næ”¯æŒä¸¤ç§æŽ¥å…¥æ–¹å¼ï¼š\nâ€¢ æ‰‹åŠ¨è¾“å…¥æ‰‹æœºå·ç™»å½•\nâ€¢ ä¸Šä¼  Telethon `.session` / `.zip` æ‰¹é‡å¯¼å…¥",
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
	return "è¯·è¾“å…¥ç§ä¿¡å·æ‰‹æœºå·ã€‚\n\næ”¯æŒæ ¼å¼ï¼š\nâ€¢ +8613800138000\nâ€¢ 8613800138000\nâ€¢ +66955305284"
}

func (s *AdminService) dmUploadPrompt() string {
	return "è¯·å‘é€ Telethon `.session` æ–‡ä»¶ï¼Œæˆ–ä¸€ä¸ªåŒ…å«å¤šä¸ª `.session` çš„ `.zip` åŽ‹ç¼©åŒ…ã€‚\n\nä¸Šä¼ åŽä¼šè‡ªåŠ¨æ‰¹é‡å¯¼å…¥å¹¶å°è¯•æ‹‰èµ·ç§ä¿¡å·ã€‚"
}

func (s *AdminService) dmAccountsText() string {
	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return "âŒ æš‚æ— ç§ä¿¡å·\n\nç‚¹å‡»â€œè¿žæŽ¥ç§ä¿¡å·â€æ‰‹åŠ¨ç™»å½•ï¼Œæˆ–ç‚¹â€œä¸Šä¼  Sessionâ€æ‰¹é‡å¯¼å…¥ã€‚"
	}

	lines := []string{fmt.Sprintf("ðŸ“‹ ç§ä¿¡å·åˆ—è¡¨ (%dä¸ª)ï¼š", len(accounts)), ""}
	for i, account := range accounts {
		status := "ðŸ”´ ç¦»çº¿"
		if account.Online {
			status = "ðŸŸ¢ åœ¨çº¿"
		}
		line := fmt.Sprintf("%d. %s %s | ä»Šæ—¥ %d æ¡", i+1, account.Phone, status, account.TodaySent)
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
		return "", nil, fmt.Errorf("ç§ä¿¡å·ä¸å­˜åœ¨")
	}

	status := "ðŸ”´ ç¦»çº¿"
	if account.Online {
		status = "ðŸŸ¢ åœ¨çº¿"
	}
	text := fmt.Sprintf("ðŸ’¬ ç§ä¿¡å·è¯¦æƒ…\n\næ‰‹æœºå·: %s\nçŠ¶æ€: %s\nSession: %s\nä»Šæ—¥å‘é€: %d æ¡\nä»Šæ—¥æˆåŠŸ: %d æ¡\nä»Šæ—¥å¤±è´¥: %d æ¡", account.Phone, status, filepathBase(account.SessionFile), account.TodaySent, account.TodaySuccess, account.TodayFailed)
	if strings.TrimSpace(account.LastError) != "" {
		text += "\né”™è¯¯: " + account.LastError
	}
	if label := dmStatusLabel(account.StatusCode); label != "" {
		text += "\nè´¦å·é™åˆ¶: " + label
	}
	if strings.TrimSpace(account.StatusSummary) != "" {
		text += "\nSpamBot æ£€æµ‹: " + account.StatusSummary
		if !account.StatusCheckedAt.IsZero() {
			text += "\næ£€æµ‹æ—¶é—´: " + account.StatusCheckedAt.Format("2006-01-02 15:04:05")
		}
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ðŸ”Ž æ£€æŸ¥çŠ¶æ€", CallbackData: callbackDMCheckPrefix + account.Phone},
				{Text: "ðŸ”„ é‡æ–°è¿žæŽ¥", CallbackData: callbackDMRetryPrefix + account.Phone},
			},
			{
				{Text: "âŒ åˆ é™¤è´¦å·", CallbackData: callbackDMDeletePrefix + account.Phone},
				{Text: "ðŸ”™ è¿”å›žåˆ—è¡¨", CallbackData: callbackDMList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) dmTemplatesText() string {
	templates := s.settings.ListDMTemplates()
	if len(templates) == 0 {
		return strings.Join([]string{
			"📝 暂无私信话术模板",
			"",
			"新增发送模式：",
			"🔴 文本直发: 0 条",
			"🔴 内联Bot @PostBot: 0 条",
			"🔴 频道贴文转发: 0 条",
			"🔴 隐藏转发来源: 0 条",
			"🔴 企业快捷回复: 0 条",
		}, "\n")
	}

	counts := countDMTemplateModes(templates)
	lines := []string{
		fmt.Sprintf("📝 话术模板 (%d条)：", len(templates)),
		"",
		fmt.Sprintf("🔴 文本直发: %d 条", counts[model.DMTemplateModeText]),
		fmt.Sprintf("🔴 内联Bot @PostBot: %d 条", counts[model.DMTemplateModePostBot]),
		fmt.Sprintf("🔴 频道贴文转发: %d 条", counts[model.DMTemplateModeForward]),
		fmt.Sprintf("🔴 隐藏转发来源: %d 条", counts[model.DMTemplateModeForwardHidden]),
		fmt.Sprintf("🔴 企业快捷回复: %d 条", counts[model.DMTemplateModeQuickReply]),
		"",
	}
	for i, item := range templates {
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, model.ParseDMTemplate(item).Summary()))
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmTemplateAddPrompt() string {
	return strings.Join([]string{
		"发送要添加的话术，多条可用 --- 分隔。",
		"",
		"支持这几种格式：",
		"1. 文本直发：直接输入内容，或用 文本::内容",
		"2. 内联Bot：PostBot::代码",
		"3. 频道贴文转发：转发::https://t.me/频道/123",
		"4. 隐藏转发来源：隐藏转发::https://t.me/频道/123",
		"5. 企业快捷回复：快捷回复::123",
	}, "\n")
}

func (s *AdminService) dmTemplateTextPrompt() string {
	return "发送文本私信内容。\n\n支持变量：{username} {chat_title} {keywords} {message}"
}

func (s *AdminService) dmTemplatePostBotPrompt() string {
	return "发送 PostBot 的内联代码。\n\n例如：abc123"
}

func (s *AdminService) dmTemplateForwardPrompt(hidden bool) string {
	if hidden {
		return "发送要隐藏来源转发的频道贴文链接。\n\n例如：https://t.me/channelname/123"
	}
	return "发送要转发的频道贴文链接。\n\n例如：https://t.me/channelname/123"
}

func (s *AdminService) dmTemplateQuickReplyPrompt() string {
	return "发送企业快捷回复 ID。\n\n例如：123"
}

func (s *AdminService) handleAddSingleDMTemplate(ctx context.Context, chatID int64, encoded, label string) error {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return s.client.SendMessage(ctx, chatID, "内容不能为空。", s.dmTemplateModeKeyboard())
	}
	added, err := s.settings.AddDMTemplates([]string{encoded})
	if err != nil {
		return err
	}
	if added > 0 {
		_ = s.settings.SetDMTemplate(encoded)
	}
	return s.client.SendMessage(ctx, chatID, fmt.Sprintf("已添加 %s 话术。\n\n%s", label, s.dmTemplatesText()), s.dmTemplatesKeyboard())
}

func (s *AdminService) dmRecordsText() string {
	records := s.recordStore.DMRecords()
	if len(records) == 0 {
		return "ðŸ“¨ æš‚æ— ç§ä¿¡è®°å½•"
	}

	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	lines := []string{
		fmt.Sprintf("ðŸ“¨ ç§ä¿¡è®°å½•\n\nä»Šæ—¥å‘é€: %d | æˆåŠŸ %d | å¤±è´¥ %d", todaySent, todaySuccess, todayFailed),
		"",
		"æœ€è¿‘ 10 æ¡ï¼š",
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
		line := fmt.Sprintf("â€¢ %s | %s | via %s", target, record.Status, senderLabel)
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
		"âš™ï¸ ç§ä¿¡å‘é€è®¾ç½®\n\nå†·å´æ—¶é—´: %d åˆ†é’Ÿ\nå‘é€æ¨¡å¼: %s\nå½“å‰é»˜è®¤æ¨¡æ¿:\n%s",
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		state.DMTemplate,
	)
}

func (s *AdminService) exportText() string {
	total := len(s.recordStore.MatchRecords())
	return fmt.Sprintf(
		"ðŸ“¤ æ•°æ®å¯¼å‡º\n\nå½“å‰å‘½ä¸­è®°å½•: %d æ¡\n\nå¯å¯¼å‡ºï¼š\nâ€¢ æŒ‰æ—¶é—´æ®µå¯¼å‡º\nâ€¢ æŒ‰å…³é”®è¯å¯¼å‡º\nâ€¢ å¯¼å‡ºå…¨éƒ¨æ•°æ®",
		total,
	)
}

func (s *AdminService) exportTimePrompt() string {
	return "ðŸ“… æŒ‰æ—¶é—´æ®µå¯¼å‡º\n\nè¯·è¾“å…¥æ—¶é—´èŒƒå›´ï¼š\nç¤ºä¾‹ 1: 09-28-12:00 | 09-28-18:30\nç¤ºä¾‹ 2: 2026-09-28 12:00 | 2026-09-28 18:30"
}

func (s *AdminService) exportKeywordPrompt() string {
	keywords := s.keywordStore.List()
	return "ðŸ”‘ æŒ‰å…³é”®è¯å¯¼å‡º\n\nå½“å‰å…³é”®è¯ï¼š\n" + strings.Join(keywords, " | ") + "\n\nè¯·è¾“å…¥è¦å¯¼å‡ºçš„å…³é”®è¯ï¼Œå¤šä¸ªç”¨ | åˆ†éš”ã€‚"
}

func (s *AdminService) legacyRulesText() string {
	state := s.settings.Snapshot()
	alertChat := "æœªè®¾ç½®"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"âš™ï¸ è¿‡æ»¤è®¾ç½®\n\nç›‘æŽ§å¼€å…³: %s\nå†·å´æ—¶é—´: %d åˆ†é’Ÿ\nå‘é€æ¨¡å¼: %s\né€šçŸ¥ç¾¤: %s\næœ€å¤§æ¶ˆæ¯é•¿åº¦: %s\nè¿‡æ»¤æ— ç”¨æˆ·å: %s\nè¿‡æ»¤æ— å¤´åƒ: %s\næœ€å°è´¦å·å¹´é¾„: %s å¤©\nç§ä¿¡æ¨¡æ¿:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MaxMessageLength, "ä¸é™"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "ä¸é™"),
		state.DMTemplate,
	)
}

func (s *AdminService) rulesText() string {
	state := s.settings.Snapshot()
	alertChat := "æœªè®¾ç½®"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"âš™ï¸ è¿‡æ»¤è®¾ç½®\n\nç›‘æŽ§å¼€å…³: %s\nåŒç”¨æˆ·é‡å¤ç§ä¿¡å†·å´: %d åˆ†é’Ÿ\nåŒç¾¤é‡å¤ç§ä¿¡å†·å´: %s åˆ†é’Ÿ\nåŒå†…å®¹é‡å¤ç§ä¿¡å†·å´: %s åˆ†é’Ÿ\nå‘é€æ¨¡å¼: %s\né€šçŸ¥ç¾¤: %s\næœ€å°æ¶ˆæ¯é•¿åº¦: %s\næœ€å¤§æ¶ˆæ¯é•¿åº¦: %s\nè¿‡æ»¤æ— ç”¨æˆ·å: %s\nè¿‡æ»¤æ— å¤´åƒ: %s\næœ€å°è´¦å·å¹´é¾„: %s å¤©\nç§ä¿¡æ¨¡æ¿:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		formatOptionalNumber(state.ChatCooldownMinutes, "å…³é—­"),
		formatOptionalNumber(state.TextCooldownMinutes, "å…³é—­"),
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MinMessageLength, "ä¸é™åˆ¶"),
		formatOptionalNumber(state.MaxMessageLength, "ä¸é™åˆ¶"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "ä¸é™åˆ¶"),
		state.DMTemplate,
	)
}

func (s *AdminService) chatsText() string {
	state := s.settings.Snapshot()
	body := "æš‚æ— ç›‘å¬ç¾¤"
	if len(state.MonitorChatIDs) > 0 {
		parts := make([]string, 0, len(state.MonitorChatIDs))
		for _, id := range state.MonitorChatIDs {
			parts = append(parts, strconv.FormatInt(id, 10))
		}
		body = strings.Join(parts, "\n")
	}
	return fmt.Sprintf("ðŸ‘‚ ç›‘å¬ç¾¤é…ç½®\n\nå½“å‰å…± %d ä¸ªï¼š\n%s", len(state.MonitorChatIDs), body)
}

func (s *AdminService) blacklistText() string {
	if s.blacklist == nil {
		return "é»‘åå•æœªå¯ç”¨"
	}

	users := s.blacklist.Users()
	userBody := "æš‚æ— é»‘åå•ç”¨æˆ·"
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
	chatBody := "æš‚æ— é»‘åå•ç¾¤"
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

	return fmt.Sprintf("ðŸš« é»‘åå•\n\nç”¨æˆ· %d ä¸ªï¼š\n%s\n\nç¾¤ %d ä¸ªï¼š\n%s", len(users), userBody, len(chats), chatBody)
}

func (s *AdminService) statusText() string {
	active, total := 0, 0
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
	}
	matchRecords := s.recordStore.MatchRecords()
	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	return fmt.Sprintf(
		"ðŸ“Š è¿è¡ŒçŠ¶æ€\n\nç›‘æŽ§è´¦å·: %dåœ¨çº¿ / %dç¦»çº¿\nå…³é”®è¯: %dä¸ª\nå‘½ä¸­è®°å½•: %d\nç§ä¿¡è®°å½•: å‘é€ %d | æˆåŠŸ %d | å¤±è´¥ %d\nè¿‡æ»¤å¼€å…³: %s\nå‘é€æ¨¡å¼: %s",
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
				{Text: "ðŸ“± ç›‘æŽ§è´¦å·", CallbackData: callbackAccounts},
				{Text: "ðŸ“ å…³é”®è¯ç®¡ç†", CallbackData: callbackKeywords},
			},
			{
				{Text: "ðŸ’¬ ç§ä¿¡å·æ± ", CallbackData: callbackDMPool},
				{Text: "ðŸ“¤ æ•°æ®å¯¼å‡º", CallbackData: callbackExport},
			},
			{
				{Text: "âš™ï¸ è¿‡æ»¤è®¾ç½®", CallbackData: callbackFilters},
				{Text: "ðŸ“Š è¿è¡ŒçŠ¶æ€", CallbackData: callbackStatus},
			},
		},
	}
}

func (s *AdminService) accountsMenuKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "âž• æ·»åŠ æ–°è´¦å·", CallbackData: callbackAccountAdd},
				{Text: "ðŸ“‹ è´¦å·åˆ—è¡¨", CallbackData: callbackAccountList},
			},
			{
				{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) accountsListKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listMonitorAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+1)
	for _, account := range accounts {
		status := "ðŸ”´"
		if account.Online {
			status = "ðŸŸ¢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackAccountDetailPrefix + account.Phone},
		})
	}
	rows = append(rows, []model.InlineKeyboardButton{
		{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackAccounts},
	})
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) backToAccountsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackAccounts}},
		},
	}
}

func (s *AdminService) keywordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "âž• æ¨¡ç³Šå…³é”®è¯", CallbackData: callbackKeywordAdd},
				{Text: "ðŸŽ¯ ç²¾å‡†å…³é”®è¯", CallbackData: callbackKeywordAdd + ":exact"},
			},
			{
				{Text: "âž– åˆ é™¤å…³é”®è¯", CallbackData: callbackKeywordRemove},
			},
			{
				{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmPoolKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ðŸ”Œ è¿žæŽ¥ç§ä¿¡å·", CallbackData: callbackDMConnect},
				{Text: "ðŸ“¤ ä¸Šä¼  Session", CallbackData: callbackDMUpload},
			},
			{
				{Text: "ðŸ“‹ è´¦å·åˆ—è¡¨", CallbackData: callbackDMList},
				{Text: "ðŸ” ä¸€é”®æ£€æŸ¥çŠ¶æ€", CallbackData: callbackDMCheckAll},
			},
			{
				{Text: "ðŸ“ è¯æœ¯æ¨¡æ¿", CallbackData: callbackDMTemplates},
				{Text: "âš™ï¸ å‘é€è®¾ç½®", CallbackData: callbackDMSettings},
			},
			{
				{Text: "ðŸ“¨ å‘é€è®°å½•", CallbackData: callbackDMRecords},
			},
			{
				{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmAccountsKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listDMAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+2)
	for _, account := range accounts {
		status := "ðŸ”´"
		if account.Online {
			status = "ðŸŸ¢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackDMDetailPrefix + account.Phone},
		})
	}
	rows = append(rows,
		[]model.InlineKeyboardButton{
			{Text: "ðŸ”Œ è¿žæŽ¥ç§ä¿¡å·", CallbackData: callbackDMConnect},
			{Text: "ðŸ“¤ ä¸Šä¼  Session", CallbackData: callbackDMUpload},
		},
		[]model.InlineKeyboardButton{{Text: "ðŸ” ä¸€é”®æ£€æŸ¥çŠ¶æ€", CallbackData: callbackDMCheckAll}},
		[]model.InlineKeyboardButton{{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackDMPool}},
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

func (s *AdminService) dmTemplateModeKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "📝 文本直发", CallbackData: callbackDMTemplateAddText},
				{Text: "🤖 内联Bot", CallbackData: callbackDMTemplateAddPost},
			},
			{
				{Text: "📢 频道转发", CallbackData: callbackDMTemplateAddFwd},
				{Text: "🕶 隐藏来源转发", CallbackData: callbackDMTemplateAddHide},
			},
			{
				{Text: "🏢 企业快捷回复", CallbackData: callbackDMTemplateAddQuick},
			},
			{
				{Text: "🔙 返回话术列表", CallbackData: callbackDMTemplates},
			},
		},
	}
}

func (s *AdminService) dmRecordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ðŸ“¤ å¯¼å‡ºå¼‚å¸¸å·", CallbackData: callbackDMExportFailed},
			},
			{
				{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmSettingsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "åˆ‡æ¢ Dry-run", CallbackData: callbackToggleDryRun},
				{Text: "è®¾ç½®å†·å´", CallbackData: callbackSetCooldown},
			},
			{
				{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) exportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ðŸ“… æŒ‰æ—¶é—´æ®µå¯¼å‡º", CallbackData: callbackExportByTime}},
			{{Text: "ðŸ”‘ æŒ‰å…³é”®è¯å¯¼å‡º", CallbackData: callbackExportByKeyword}},
			{{Text: "ðŸ“‹ å¯¼å‡ºå…¨éƒ¨æ•°æ®", CallbackData: callbackExportAll}},
			{{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) exportFormatKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ðŸ‘¤ ä»…ç”¨æˆ·å (TXT)", CallbackData: callbackExportFormatUsers}},
			{{Text: "ðŸ†” ä»…ç”¨æˆ·ID (TXT)", CallbackData: callbackExportFormatIDs}},
			{{Text: "ðŸ“Š å®Œæ•´è®°å½• (CSV)", CallbackData: callbackExportFormatCSV}},
			{{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) cancelExportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ðŸ”™ å–æ¶ˆ", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) legacyRulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "å¼€å…³ç›‘æŽ§", CallbackData: callbackToggleMonitor}, {Text: "åˆ‡æ¢ Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "è®¾ç½®å†·å´", CallbackData: callbackSetCooldown}, {Text: "è®¾ç½®æ¨¡æ¿", CallbackData: callbackSetTemplate}},
			{{Text: "æœ€å¤§æ¶ˆæ¯é•¿åº¦", CallbackData: callbackSetMaxLength}, {Text: "æœ€å°è´¦å·å¹´é¾„", CallbackData: callbackSetMinAge}},
			{{Text: "è¿‡æ»¤æ— ç”¨æˆ·å", CallbackData: callbackToggleNoName}, {Text: "è¿‡æ»¤æ— å¤´åƒ", CallbackData: callbackToggleNoPhoto}},
			{{Text: "ç›‘å¬ç¾¤é…ç½®", CallbackData: callbackChats}, {Text: "é»‘åå•", CallbackData: callbackBlacklist}},
			{{Text: "å½“å‰èŠå¤©è®¾ä¸ºé€šçŸ¥ç¾¤", CallbackData: callbackSetAlertChat}},
			{{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) rulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "å¼€å…³ç›‘æŽ§", CallbackData: callbackToggleMonitor}, {Text: "åˆ‡æ¢ Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "åŒç”¨æˆ·å†·å´", CallbackData: callbackSetCooldown}, {Text: "åŒç¾¤å†·å´", CallbackData: callbackSetChatCooldown}},
			{{Text: "åŒå†…å®¹å†·å´", CallbackData: callbackSetTextCooldown}, {Text: "è®¾ç½®æ¨¡æ¿", CallbackData: callbackSetTemplate}},
			{{Text: "æœ€å°æ¶ˆæ¯é•¿åº¦", CallbackData: callbackSetMinLength}, {Text: "æœ€å¤§æ¶ˆæ¯é•¿åº¦", CallbackData: callbackSetMaxLength}},
			{{Text: "æœ€å°è´¦å·å¹´é¾„", CallbackData: callbackSetMinAge}, {Text: "è¿‡æ»¤æ— ç”¨æˆ·å", CallbackData: callbackToggleNoName}},
			{{Text: "è¿‡æ»¤æ— å¤´åƒ", CallbackData: callbackToggleNoPhoto}},
			{{Text: "ç›‘å¬ç¾¤é…ç½®", CallbackData: callbackChats}, {Text: "é»‘åå•", CallbackData: callbackBlacklist}},
			{{Text: "å½“å‰èŠå¤©è®¾ä¸ºé€šçŸ¥ç¾¤", CallbackData: callbackSetAlertChat}},
			{{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) chatsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "âž• æ·»åŠ ç›‘å¬ç¾¤", CallbackData: callbackAddChat}, {Text: "ðŸ—‘ ç§»é™¤ç›‘å¬ç¾¤", CallbackData: callbackRemoveChat}},
			{{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) blacklistKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ç§»å‡ºé»‘åå•ç”¨æˆ·", CallbackData: callbackUnblockUser}, {Text: "ç§»å‡ºé»‘åå•ç¾¤", CallbackData: callbackUnblockChat}},
			{{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) statusKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ðŸ”™ è¿”å›žä¸»èœå•", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) blockUserCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "é»‘åå•æœªå¯ç”¨", nil
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
		return "ç”¨æˆ·å·²åœ¨é»‘åå•", nil
	}
	return "å·²æ‹‰é»‘ç”¨æˆ·", nil
}

func (s *AdminService) blockChatCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "é»‘åå•æœªå¯ç”¨", nil
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
		return "ç¾¤å·²åœ¨é»‘åå•", nil
	}
	return "å·²æ‹‰é»‘ç¾¤", nil
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
		return fmt.Errorf("è¯·å…ˆé€‰æ‹©å¯¼å‡ºæ¡ä»¶")
	}

	records := s.filterMatchRecords(exportCtx)
	if len(records) == 0 {
		return fmt.Errorf("æ²¡æœ‰åŒ¹é…åˆ°å¯å¯¼å‡ºçš„æ•°æ®")
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
		return fmt.Errorf("æœªçŸ¥å¯¼å‡ºæ ¼å¼")
	}
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("å¯¼å‡ºç»“æžœä¸ºç©º")
	}

	caption := fmt.Sprintf("å¯¼å‡ºå®Œæˆï¼Œå…± %d æ¡è®°å½•ã€‚", len(records))
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
	if err := writer.Write([]string{"ç”¨æˆ·ID", "ç”¨æˆ·å", "æ˜µç§°", "æ¥æºç¾¤ç»„", "è§¦å‘å…³é”®è¯", "è§¦å‘æ—¶é—´", "æ¶ˆæ¯å†…å®¹", "ç›‘æŽ§è´¦å·"}); err != nil {
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

func parseDMTemplateInputs(text string) ([]string, error) {
	parts := splitTemplateParts(text)
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		encoded, err := encodeDMTemplateInput(part)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded)
	}
	return result, nil
}

func encodeDMTemplateInput(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", nil
	}

	parts := strings.SplitN(input, "::", 2)
	if len(parts) < 2 {
		return model.EncodeTextDMTemplate(input), nil
	}

	mode := strings.ToLower(strings.TrimSpace(parts[0]))
	value := strings.TrimSpace(parts[1])
	if value == "" {
		return "", fmt.Errorf("发送模式内容不能为空")
	}

	switch mode {
	case "文本", "文本直发", "text":
		return model.EncodeTextDMTemplate(value), nil
	case "postbot", "内联bot", "内联":
		return model.EncodePostBotDMTemplate(value), nil
	case "转发", "频道转发", "频道贴文转发", "forward":
		return model.EncodeForwardDMTemplate(value), nil
	case "隐藏转发", "隐藏来源", "隐藏转发来源", "forward_hidden", "hidden_forward":
		return model.EncodeHiddenForwardDMTemplate(value), nil
	case "快捷回复", "企业快捷回复", "quick_reply", "quickreply":
		shortcutID, err := strconv.Atoi(value)
		if err != nil || shortcutID <= 0 {
			return "", fmt.Errorf("企业快捷回复格式不对，请用 快捷回复::123 这种格式")
		}
		return model.EncodeQuickReplyDMTemplate(shortcutID), nil
	default:
		return "", fmt.Errorf("不支持的发送模式：%s", strings.TrimSpace(parts[0]))
	}
}

func resolveDMTemplateRemovals(text string, existing []string) []string {
	parts := splitInputParts(text)
	if len(parts) == 0 || len(existing) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if index, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
			if index >= 1 && index <= len(existing) {
				item := existing[index-1]
				if _, ok := seen[item]; !ok {
					seen[item] = struct{}{}
					result = append(result, item)
				}
			}
			continue
		}

		for _, item := range existing {
			payload := model.ParseDMTemplate(item)
			if strings.EqualFold(strings.TrimSpace(part), strings.TrimSpace(item)) || strings.EqualFold(strings.TrimSpace(part), strings.TrimSpace(payload.Summary())) {
				if _, ok := seen[item]; !ok {
					seen[item] = struct{}{}
					result = append(result, item)
				}
				break
			}
		}
	}
	return result
}

func countDMTemplateModes(templates []string) map[string]int {
	counts := map[string]int{
		model.DMTemplateModeText:          0,
		model.DMTemplateModePostBot:       0,
		model.DMTemplateModeForward:       0,
		model.DMTemplateModeForwardHidden: 0,
		model.DMTemplateModeQuickReply:    0,
	}
	for _, item := range templates {
		mode := model.ParseDMTemplate(item).Mode
		if _, ok := counts[mode]; !ok {
			mode = model.DMTemplateModeText
		}
		counts[mode]++
	}
	return counts
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
		return "å¼€å¯"
	}
	return "å…³é—­"
}

func formatOptionalNumber(value int, disabled string) string {
	if value <= 0 {
		return disabled
	}
	return strconv.Itoa(value)
}

func dryRunLabel(enabled bool) string {
	if enabled {
		return "æ¼”ç»ƒæ¨¡å¼ï¼ˆä¸çœŸå®žç§ä¿¡ï¼‰"
	}
	return "çœŸå®žå‘é€"
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
	return string(runes[:limit-12]) + "\n\nâ€¦â€¦ å†…å®¹è¿‡é•¿ï¼Œå·²æˆªæ–­"
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
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "å½“å‰æ²¡æœ‰å¯ç”¨çš„ç§ä¿¡å·ç®¡ç†å™¨")
	}

	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "å½“å‰æ²¡æœ‰ç§ä¿¡å·å¯æ£€æŸ¥")
	}

	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "å¼€å§‹æ‰¹é‡æ£€æŸ¥ç§ä¿¡å·çŠ¶æ€"); err != nil {
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
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "å¼€å§‹å¯¼å‡ºå¼‚å¸¸ç§ä¿¡å·"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "âœ… å½“å‰æ²¡æœ‰å¼‚å¸¸ç§ä¿¡å·ï¼Œæ‰€æœ‰è´¦å·éƒ½å±žäºŽæ­£å¸¸æ— é™åˆ¶çŠ¶æ€ã€‚", s.dmCheckActionsKeyboard())
	}

	if err := s.sendDMAccountExportDocuments(ctx, callback.Message.Chat.ID, accounts, "å¼‚å¸¸ç§ä¿¡å·"); err != nil {
		return err
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("ðŸ“¤ å¼‚å¸¸è´¦å·å·²å¯¼å‡º\n\nå·²å¯¼å‡º: %d ä¸ª\nå·²åˆ é™¤: %d ä¸ª\nåˆ é™¤å¤±è´¥: %d ä¸ª\n\nå·²ä¿ç•™: ä»…æ­£å¸¸æ— é™åˆ¶è´¦å·", len(accounts), removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) handleDMKeepOnlyNormal(ctx context.Context, callback *model.CallbackQuery) error {
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "å¼€å§‹æ¸…ç†å¼‚å¸¸ç§ä¿¡å·"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "âœ… å½“å‰æ²¡æœ‰å¼‚å¸¸ç§ä¿¡å·ï¼Œå·²ç»åªä¿ç•™æ­£å¸¸æ— é™åˆ¶è´¦å·ã€‚", s.dmAccountsKeyboard())
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("ðŸ§¹ ç§ä¿¡å·æ± å·²æ¸…ç†\n\nå·²åˆ é™¤å¼‚å¸¸è´¦å·: %d ä¸ª\nåˆ é™¤å¤±è´¥: %d ä¸ª\nå½“å‰åªä¿ç•™æ­£å¸¸æ— é™åˆ¶è´¦å·ã€‚", removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) dmCheckProgressText(done, total int, counts map[string]int) string {
	return fmt.Sprintf(
		"ðŸ” æ­£åœ¨æ£€æŸ¥ç§ä¿¡å·çŠ¶æ€ (%d/%d)\n\n%s",
		done,
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckResultText(total int, counts map[string]int) string {
	return fmt.Sprintf(
		"âœ… ç§ä¿¡å·çŠ¶æ€æ£€æŸ¥å®Œæˆ\n\næ€»è®¡: %d ä¸ªè´¦å·\n%s\nâš ï¸ æŽ¥ä¸‹æ¥ä½ å¯ä»¥å¯¼å‡ºå¼‚å¸¸è´¦å·ï¼Œæˆ–è€…ç›´æŽ¥åªä¿ç•™æ­£å¸¸æ— é™åˆ¶è´¦å·ã€‚",
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckActionsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ðŸ“¤ å¯¼å‡ºå¼‚å¸¸å¹¶åˆ é™¤", CallbackData: callbackDMExportAbnormal},
				{Text: "ðŸ§¹ ä»…ä¿ç•™æ­£å¸¸è´¦å·", CallbackData: callbackDMKeepOnlyNormal},
			},
			{
				{Text: "ðŸ“‹ è¿”å›žè´¦å·åˆ—è¡¨", CallbackData: callbackDMList},
				{Text: "ðŸ”™ è¿”å›žç§ä¿¡å·æ± ", CallbackData: callbackDMPool},
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
		caption := fmt.Sprintf("ðŸ“¦ %s Session æ‰“åŒ…ï¼ˆ%d ä¸ªï¼‰", label, sessionCount)
		if err := s.client.SendDocument(ctx, chatID, filename, zipData, caption); err != nil {
			return err
		}
	}

	reportData := buildDMAccountReport(accounts, label)
	reportName := fmt.Sprintf("dm_abnormal_accounts_%s.txt", timestamp)
	reportCaption := fmt.Sprintf("ðŸ“‹ %såˆ—è¡¨ï¼ˆ%d ä¸ªï¼‰", label, len(accounts))
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
		fmt.Sprintf("# å¯¼å‡ºæ—¶é—´: %s", time.Now().Format("2006-01-02 15:04:05")),
		fmt.Sprintf("# å…± %d ä¸ªè´¦å·", len(accounts)),
		"",
	}

	for _, account := range accounts {
		code := effectiveDMStatusCode(account)
		line := fmt.Sprintf(
			"%s | %s | ä»Šæ—¥å‘é€ %d æ¡ | ä»Šæ—¥æˆåŠŸ %d æ¡ | ä»Šæ—¥å¤±è´¥ %d æ¡",
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
		fmt.Sprintf("âœ… æ­£å¸¸æ— é™åˆ¶: %d", counts["active"]),
		fmt.Sprintf("âš ï¸ ä¸´æ—¶é™åˆ¶ / åŒå‘é™åˆ¶: %d", counts["restricted"]),
		fmt.Sprintf("ðŸ“µ åžƒåœ¾æ¶ˆæ¯é£ŽæŽ§: %d", counts["spam"]),
		fmt.Sprintf("ðŸš« å°ç¦è´¦å·: %d", counts["banned"]),
		fmt.Sprintf("â„ï¸ å†»ç»“ / å®¡æ ¸ä¸­: %d", counts["frozen"]),
		fmt.Sprintf("ðŸ”Œ ç¦»çº¿ / æ£€æŸ¥å¤±è´¥: %d", counts["failed"]),
	}
	if counts["unknown"] > 0 {
		lines = append(lines, fmt.Sprintf("â“ æœªè¯†åˆ«çŠ¶æ€: %d", counts["unknown"]))
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
	case strings.Contains(summary, "æ­£å¸¸"), strings.Contains(summary, "æ— é™åˆ¶"):
		return "active"
	case strings.Contains(summary, "åŒå‘"), strings.Contains(summary, "ä¸´æ—¶é™åˆ¶"):
		return "restricted"
	case strings.Contains(summary, "é£ŽæŽ§"), strings.Contains(summary, "åžƒåœ¾æ¶ˆæ¯"):
		return "spam"
	case strings.Contains(summary, "å°ç¦"), strings.Contains(summary, "æ°¸ä¹…é™åˆ¶"):
		return "banned"
	case strings.Contains(summary, "å®¡æ ¸"), strings.Contains(summary, "éªŒè¯"), strings.Contains(summary, "å†»ç»“"):
		return "frozen"
	case strings.Contains(summary, "å¤±è´¥"), strings.Contains(summary, "ç¦»çº¿"):
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
		return "âœ… æ­£å¸¸æ— é™åˆ¶"
	case "restricted":
		return "âš ï¸ ä¸´æ—¶é™åˆ¶ / åŒå‘é™åˆ¶"
	case "spam":
		return "ðŸ“µ åžƒåœ¾æ¶ˆæ¯é£ŽæŽ§"
	case "banned":
		return "ðŸš« å°ç¦"
	case "frozen":
		return "â„ï¸ å†»ç»“ / å®¡æ ¸ä¸­"
	case "failed":
		return "ðŸ”Œ ç¦»çº¿ / æ£€æŸ¥å¤±è´¥"
	default:
		return "â“ æœªè¯†åˆ«"
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
		return fmt.Errorf("æ²¡æœ‰å¼‚å¸¸ç§ä¿¡è®°å½•")
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
	return s.client.SendDocument(ctx, chatID, filename, []byte(strings.Join(lines, "\n")), fmt.Sprintf("å¼‚å¸¸ç§ä¿¡è®°å½• %d æ¡", len(failed)))
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

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
			return true, s.client.SendMessage(ctx, msg.Chat.ID, "Ã¨Â¯Â·Ã¥Ââ€˜Ã©â‚¬Â `.session` Ã¦Ë†â€“ `.zip` Ã¦â€“â€¡Ã¤Â»Â¶Ã£â‚¬â€š", s.dmPoolKeyboard())
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
		return true, s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â½â€œÃ¥â€°ÂÃ¨ÂÅ Ã¥Â¤Â© ID: %d", msg.Chat.ID), nil)
	}

	if s.authInput != nil && !strings.HasPrefix(text, "/") {
		if handled, kind := s.authInput.Submit(text); handled {
			ack := "Ã¥Â·Â²Ã¦â€Â¶Ã¥Ë†Â°Ã§â„¢Â»Ã¥Â½â€¢Ã©ÂªÅ’Ã¨Â¯ÂÃ§Â ÂÃ¯Â¼Å’Ã¦Â­Â£Ã¥Å“Â¨Ã§Â»Â§Ã§Â»Â­Ã§â„¢Â»Ã¥Â½â€¢Ã£â‚¬â€š"
			if kind == "password" {
				ack = "Ã¥Â·Â²Ã¦â€Â¶Ã¥Ë†Â°Ã¤Â¸Â¤Ã¦Â­Â¥Ã©ÂªÅ’Ã¨Â¯ÂÃ¥Â¯â€ Ã§Â ÂÃ¯Â¼Å’Ã¦Â­Â£Ã¥Å“Â¨Ã§Â»Â§Ã§Â»Â­Ã§â„¢Â»Ã¥Â½â€¢Ã£â‚¬â€š"
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
		return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¦Â²Â¡Ã¦Å“â€°Ã¦Â¶Ë†Ã¦ÂÂ¯Ã¤Â¸Å Ã¤Â¸â€¹Ã¦â€“â€¡")
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
		alert = "Ã¦Å Å Ã¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã§â€ºÂ´Ã¦Å½Â¥Ã¥Ââ€˜Ã§Â»â„¢Ã¦Ë†â€˜"
	case callbackAccountList:
		text, keyboard = s.accountsListText(), s.accountsListKeyboard()
	case callbackKeywords:
		text, keyboard = s.keywordsText(), s.keywordsKeyboard()
	case callbackKeywordAdd:
		s.setPending(callback.From.ID, pendingAddKeywordsFuzzy)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¦Â·Â»Ã¥Å Â Ã§Å¡â€žÃ¦Â¨Â¡Ã§Â³Å Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š\n\nÃ¦Â¨Â¡Ã§Â³Å Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å¡Ã¤Â¸â‚¬Ã¥ÂÂ¥Ã¨Â¯ÂÃ©â€¡Å’Ã¥ÂÂªÃ¨Â¦ÂÃ¥Å’â€¦Ã¥ÂÂ«Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â°Â±Ã¥â€˜Â½Ã¤Â¸Â­Ã£â‚¬â€š", s.keywordsKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã§Â³Å Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â"
	case callbackKeywordAdd + ":exact":
		s.setPending(callback.From.ID, pendingAddKeywordsExact)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¦Â·Â»Ã¥Å Â Ã§Å¡â€žÃ§Â²Â¾Ã¥â€¡â€ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š\n\nÃ§Â²Â¾Ã¥â€¡â€ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å¡Ã¦Â¶Ë†Ã¦ÂÂ¯Ã¥â€ â€¦Ã¥Â®Â¹Ã¥Â¿â€¦Ã©Â¡Â»Ã¤Â¸Å½Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â®Å’Ã¥â€¦Â¨Ã¤Â¸â‚¬Ã¨â€¡Â´Ã¦â€°ÂÃ¥â€˜Â½Ã¤Â¸Â­Ã£â‚¬â€š", s.keywordsKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ§Â²Â¾Ã¥â€¡â€ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â"
	case callbackKeywordRemove:
		s.setPending(callback.From.ID, pendingRemoveKeyword)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¥Ë†Â Ã©â„¢Â¤Ã§Å¡â€žÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š\n\nÃ¦â€Â¯Ã¦Å’ÂÃ¯Â¼Å¡\nÃ§Â²Â¾Ã¥â€¡â€ :Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â\nÃ¦Â¨Â¡Ã§Â³Å :Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â\nÃ¦Ë†â€“Ã§â€ºÂ´Ã¦Å½Â¥Ã¥Ââ€˜Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¦â€“â€¡Ã¦Å“Â¬Ã¯Â¼Ë†Ã¤Â¼Å¡Ã¥Ë†Â Ã©â„¢Â¤Ã¥ÂÅ’Ã¥ÂÂÃ§Å¡â€žÃ§Â²Â¾Ã¥â€¡â€ /Ã¦Â¨Â¡Ã§Â³Å Ã¨Â§â€žÃ¥Ë†â„¢Ã¯Â¼â€°Ã£â‚¬â€š", s.keywordsKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¥Ë†Â Ã©â„¢Â¤Ã§Å¡â€žÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯Â"
	case callbackDMPool:
		text, keyboard = s.dmPoolText(), s.dmPoolKeyboard()
	case callbackDMConnect:
		s.setPending(callback.From.ID, pendingLoginDM)
		text, keyboard = s.dmConnectPrompt(), s.dmPoolKeyboard()
		alert = "Ã¦Å Å Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã§â€ºÂ´Ã¦Å½Â¥Ã¥Ââ€˜Ã§Â»â„¢Ã¦Ë†â€˜"
	case callbackDMUpload:
		s.setPending(callback.From.ID, pendingUploadDMSess)
		text, keyboard = s.dmUploadPrompt(), s.dmPoolKeyboard()
		alert = "Ã¦Å Å  session Ã¦â€“â€¡Ã¤Â»Â¶Ã§â€ºÂ´Ã¦Å½Â¥Ã¥Ââ€˜Ã§Â»â„¢Ã¦Ë†â€˜"
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
		text, keyboard = "é€‰æ‹©è¦æ·»åŠ çš„è¯æœ¯å‘é€æ¨¡å¼ã€‚", s.dmTemplateModeKeyboard()
		alert = "è¯·é€‰æ‹©è¯æœ¯å‘é€æ¨¡å¼"
	case callbackDMTemplateAddText:
		s.setPending(callback.From.ID, pendingAddDMText)
		text, keyboard = s.dmTemplateTextPrompt(), s.dmTemplateModeKeyboard()
		alert = "å‘é€æ–‡æœ¬è¯æœ¯å†…å®¹"
	case callbackDMTemplateAddPost:
		s.setPending(callback.From.ID, pendingAddDMPostBot)
		text, keyboard = s.dmTemplatePostBotPrompt(), s.dmTemplateModeKeyboard()
		alert = "å‘é€ PostBot ä»£ç "
	case callbackDMTemplateAddFwd:
		s.setPending(callback.From.ID, pendingAddDMForward)
		text, keyboard = s.dmTemplateForwardPrompt(false), s.dmTemplateModeKeyboard()
		alert = "å‘é€é¢‘é“è´´æ–‡é“¾æŽ¥"
	case callbackDMTemplateAddHide:
		s.setPending(callback.From.ID, pendingAddDMHidden)
		text, keyboard = s.dmTemplateForwardPrompt(true), s.dmTemplateModeKeyboard()
		alert = "å‘é€éšè—æ¥æºè½¬å‘é“¾æŽ¥"
	case callbackDMTemplateAddQuick:
		s.setPending(callback.From.ID, pendingAddDMQuickReply)
		text, keyboard = s.dmTemplateQuickReplyPrompt(), s.dmTemplateModeKeyboard()
		alert = "å‘é€å¿«æ·å›žå¤ ID"
	case callbackDMTemplateRemove:
		s.setPending(callback.From.ID, pendingRemoveDMTpl)
		text, keyboard = "å‘é€è¦åˆ é™¤çš„è¯æœ¯ç¼–å·æˆ–å†…å®¹ï¼Œæ”¯æŒä¸€æ¬¡åˆ é™¤å¤šæ¡ï¼Œä½¿ç”¨ |ã€æ¢è¡Œæˆ–é€—å·åˆ†éš”ã€‚", s.dmTemplatesKeyboard()
		alert = "ç­‰å¾…ä½ å‘é€è¦åˆ é™¤çš„è¯æœ¯"
	case callbackDMRecords:
		text, keyboard = s.dmRecordsText(), s.dmRecordsKeyboard()
	case callbackDMExportFailed:
		err = s.sendFailedDMExport(ctx, callback.Message.Chat.ID)
		alert = "Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¨Â®Â°Ã¥Â½â€¢Ã¥Â·Â²Ã¥Â¯Â¼Ã¥â€¡Âº"
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
		text, keyboard = "Ã¥Â·Â²Ã©â‚¬â€°Ã¦â€¹Â©Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥â€¦Â¨Ã©Æ’Â¨Ã¦â€¢Â°Ã¦ÂÂ®Ã¯Â¼Å’Ã¨Â¯Â·Ã©â‚¬â€°Ã¦â€¹Â©Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦Â Â¼Ã¥Â¼ÂÃ£â‚¬â€š", s.exportFormatKeyboard()
	case callbackExportFormatUsers, callbackExportFormatIDs, callbackExportFormatCSV:
		err = s.sendExportFile(ctx, callback.Message.Chat.ID, callback.From.ID, callback.Data)
		alert = "Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦â€“â€¡Ã¤Â»Â¶Ã¥Â·Â²Ã¥Ââ€˜Ã©â‚¬Â"
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
				alert = "Ã§â€ºâ€˜Ã¦Å½Â§Ã¥Â·Â²Ã¥Â¼â‚¬Ã¥ÂÂ¯"
			} else {
				alert = "Ã§â€ºâ€˜Ã¦Å½Â§Ã¥Â·Â²Ã¥â€¦Â³Ã©â€”Â­"
			}
		}
	case callbackToggleDryRun:
		var enabled bool
		enabled, err = s.settings.ToggleDryRun()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "Ã¥Â·Â²Ã¥Ë†â€¡Ã¥Ë†Â° dry-run"
			} else {
				alert = "Ã¥Â·Â²Ã¥Ë†â€¡Ã¥Ë†Â°Ã§Å“Å¸Ã¥Â®Å¾Ã¥Ââ€˜Ã©â‚¬Â"
			}
		}
	case callbackSetCooldown:
		s.setPending(callback.From.ID, pendingSetCooldown)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦â€“Â°Ã§Å¡â€žÃ¥ÂÅ’Ã§â€Â¨Ã¦Ë†Â·Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Ë†â€ Ã©â€™Å¸Ã¦â€¢Â°Ã¯Â¼Å’Ã¦Â¯â€Ã¥Â¦â€š 1440Ã£â‚¬â€š", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¥â€ Â·Ã¥ÂÂ´Ã¥Ë†â€ Ã©â€™Å¸Ã¦â€¢Â°"
	case callbackSetChatCooldown:
		s.setPending(callback.From.ID, pendingSetChatCooldown)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¥ÂÅ’Ã§Â¾Â¤Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Ë†â€ Ã©â€™Å¸Ã¦â€¢Â°Ã¯Â¼Å’Ã¥Â¡Â« 0 Ã¨Â¡Â¨Ã§Â¤ÂºÃ¥â€¦Â³Ã©â€”Â­Ã£â‚¬â€š", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¥ÂÅ’Ã§Â¾Â¤Ã¥â€ Â·Ã¥ÂÂ´Ã¥Ë†â€ Ã©â€™Å¸Ã¦â€¢Â°"
	case callbackSetTextCooldown:
		s.setPending(callback.From.ID, pendingSetTextCooldown)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¥ÂÅ’Ã¥â€ â€¦Ã¥Â®Â¹Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Ë†â€ Ã©â€™Å¸Ã¦â€¢Â°Ã¯Â¼Å’Ã¥Â¡Â« 0 Ã¨Â¡Â¨Ã§Â¤ÂºÃ¥â€¦Â³Ã©â€”Â­Ã£â‚¬â€š", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¥ÂÅ’Ã¥â€ â€¦Ã¥Â®Â¹Ã¥â€ Â·Ã¥ÂÂ´Ã¥Ë†â€ Ã©â€™Å¸Ã¦â€¢Â°"
	case callbackSetTemplate:
		s.setPending(callback.From.ID, pendingSetTemplate)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦â€“Â°Ã§Å¡â€žÃ§Â§ÂÃ¤Â¿Â¡Ã¦Â¨Â¡Ã¦ÂÂ¿Ã£â‚¬â€šÃ¥ÂÂ¯Ã§â€Â¨Ã¥ÂËœÃ©â€¡Â: {username} {chat_title} {keywords} {message}", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¦Â¨Â¡Ã¦ÂÂ¿"
	case callbackSetMinLength:
		s.setPending(callback.From.ID, pendingSetMinLength)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Å“â‚¬Ã¥Â°ÂÃ¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦Ã¯Â¼Å’Ã¥Â¡Â« 0 Ã¨Â¡Â¨Ã§Â¤ÂºÃ¤Â¸ÂÃ©â„¢ÂÃ¥Ë†Â¶Ã£â‚¬â€š", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Å“â‚¬Ã¥Â°ÂÃ¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦"
	case callbackSetMaxLength:
		s.setPending(callback.From.ID, pendingSetMaxLength)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦Ã¯Â¼Å’Ã¥Â¡Â« 0 Ã¨Â¡Â¨Ã§Â¤ÂºÃ¤Â¸ÂÃ©â„¢ÂÃ¥Ë†Â¶Ã£â‚¬â€š", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦"
	case callbackSetMinAge:
		s.setPending(callback.From.ID, pendingSetMinAge)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Å“â‚¬Ã¥Â°ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€žÃ¥Â¤Â©Ã¦â€¢Â°Ã¯Â¼Å’Ã¥Â¡Â« 0 Ã¨Â¡Â¨Ã§Â¤ÂºÃ¤Â¸ÂÃ©â„¢ÂÃ¥Ë†Â¶Ã£â‚¬â€š", s.rulesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€ž"
	case callbackToggleNoName:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoUsername()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "Ã¥Â·Â²Ã¥Â¼â‚¬Ã¥ÂÂ¯Ã¦â€”Â Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂÃ¨Â¿â€¡Ã¦Â»Â¤"
			} else {
				alert = "Ã¥Â·Â²Ã¥â€¦Â³Ã©â€”Â­Ã¦â€”Â Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂÃ¨Â¿â€¡Ã¦Â»Â¤"
			}
		}
	case callbackToggleNoPhoto:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoAvatar()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "Ã¥Â·Â²Ã¥Â¼â‚¬Ã¥ÂÂ¯Ã¦â€”Â Ã¥Â¤Â´Ã¥Æ’ÂÃ¨Â¿â€¡Ã¦Â»Â¤"
			} else {
				alert = "Ã¥Â·Â²Ã¥â€¦Â³Ã©â€”Â­Ã¦â€”Â Ã¥Â¤Â´Ã¥Æ’ÂÃ¨Â¿â€¡Ã¦Â»Â¤"
			}
		}
	case callbackChats:
		text, keyboard = s.chatsText(), s.chatsKeyboard()
	case callbackAddChat:
		s.setPending(callback.From.ID, pendingAddChatIDs)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¦Â·Â»Ã¥Å Â Ã§Å¡â€žÃ§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤ IDÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š", s.chatsKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ§Â¾Â¤ ID"
	case callbackRemoveChat:
		s.setPending(callback.From.ID, pendingRemoveChatIDs)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ§Â§Â»Ã©â„¢Â¤Ã§Å¡â€žÃ§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤ IDÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š", s.chatsKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ§Â§Â»Ã©â„¢Â¤Ã§Å¡â€žÃ§Â¾Â¤ ID"
	case callbackSetAlertChat:
		err = s.settings.SetAlertChatID(callback.Message.Chat.ID)
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			alert = "Ã¥Â½â€œÃ¥â€°ÂÃ¨ÂÅ Ã¥Â¤Â©Ã¥Â·Â²Ã¨Â®Â¾Ã¤Â¸ÂºÃ©â‚¬Å¡Ã§Å¸Â¥Ã§Â¾Â¤"
		}
	case callbackBlacklist:
		text, keyboard = s.blacklistText(), s.blacklistKeyboard()
	case callbackUnblockUser:
		s.setPending(callback.From.ID, pendingUnblockUsers)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ§Â§Â»Ã¥â€¡ÂºÃ©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§Å¡â€žÃ§â€Â¨Ã¦Ë†Â· IDÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š", s.blacklistKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ§â€Â¨Ã¦Ë†Â· ID"
	case callbackUnblockChat:
		s.setPending(callback.From.ID, pendingUnblockChats)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ§Â§Â»Ã¥â€¡ÂºÃ©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§Å¡â€žÃ§Â¾Â¤ IDÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¦Ë†â€“Ã¦ÂÂ¢Ã¨Â¡Å’Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š", s.blacklistKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ§Â¾Â¤ ID"
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
				alert = "Ã¥Â·Â²Ã©â€¡ÂÃ¦â€“Â°Ã¥Ââ€˜Ã¨ÂµÂ·Ã¨Â¿Å¾Ã¦Å½Â¥"
				text, keyboard, _ = s.accountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackAccountDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackAccountDeletePrefix)
			err = s.monitorManager.DeleteMonitor(ctx, phone)
			if err == nil {
				alert = "Ã§â€ºâ€˜Ã¦Å½Â§Ã¥ÂÂ·Ã¥Â·Â²Ã¥Ë†Â Ã©â„¢Â¤"
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
				alert = "Ã¥Â·Â²Ã©â€¡ÂÃ¦â€“Â°Ã¨Â¿Å¾Ã¦Å½Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·"
				text, keyboard, _ = s.dmAccountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackDMDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMDeletePrefix)
			err = s.dmManager.DeleteDMAccount(ctx, phone)
			if err == nil {
				alert = "Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¥Â·Â²Ã¥Ë†Â Ã©â„¢Â¤"
				text, keyboard = s.dmAccountsText(), s.dmAccountsKeyboard()
			}
		case strings.HasPrefix(callback.Data, callbackBlockUser):
			alert, err = s.blockUserCallback(callback.Data)
		case strings.HasPrefix(callback.Data, callbackBlockChat):
			alert, err = s.blockChatCallback(callback.Data)
		default:
			return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¦Å“ÂªÃ§Å¸Â¥Ã¦â€œÂÃ¤Â½Å“")
		}
	}

	if err == nil && text != "" {
		err = s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, keyboard)
	}

	answerText := alert
	if err != nil {
		answerText = "Ã¦â€œÂÃ¤Â½Å“Ã¥Â¤Â±Ã¨Â´Â¥: " + err.Error()
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
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©ÂºÃ£â‚¬â€š", s.backToAccountsKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¤Â¸â‚¬Ã¦Â¬Â¡Ã¥â€¦Ë†Ã§â„¢Â»Ã¥Â½â€¢Ã¤Â¸â‚¬Ã¤Â¸ÂªÃ§â€ºâ€˜Ã¦Å½Â§Ã¥ÂÂ·Ã£â‚¬â€šÃ¥Â¤Å¡Ã¤Â¸ÂªÃ§â€ºâ€˜Ã¦Å½Â§Ã¥ÂÂ·Ã¨Â¯Â·Ã©â‚¬ÂÃ¤Â¸ÂªÃ¦Â·Â»Ã¥Å Â Ã£â‚¬â€š", s.backToAccountsKeyboard())
		}
		if s.monitorManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã¥ÂÂ¯Ã§â€Â¨Ã§Å¡â€žÃ§â€ºâ€˜Ã¦Å½Â§Ã¨Â´Â¦Ã¥ÂÂ·Ã§Â®Â¡Ã§Ââ€ Ã¥â„¢Â¨Ã£â‚¬â€š", s.backToAccountsKeyboard())
		}
		result, err := s.monitorManager.StartMonitorLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÂ¯Ã¥Å Â¨Ã§â„¢Â»Ã¥Â½â€¢Ã¥Â¤Â±Ã¨Â´Â¥: "+err.Error(), s.backToAccountsKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.accountsOverviewText(), s.accountsMenuKeyboard())
	case pendingLoginDM:
		phones := splitInputParts(text)
		if len(phones) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©ÂºÃ£â‚¬â€š", s.dmPoolKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¤Â¸â‚¬Ã¦Â¬Â¡Ã¥â€¦Ë†Ã§â„¢Â»Ã¥Â½â€¢Ã¤Â¸â‚¬Ã¤Â¸ÂªÃ§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã£â‚¬â€šÃ¥Â¤Å¡Ã¤Â¸ÂªÃ§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¨Â¯Â·Ã©â‚¬ÂÃ¤Â¸ÂªÃ¦Â·Â»Ã¥Å Â Ã£â‚¬â€š", s.dmPoolKeyboard())
		}
		if s.dmManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã¥ÂÂ¯Ã§â€Â¨Ã§Å¡â€žÃ§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§Â®Â¡Ã§Ââ€ Ã¥â„¢Â¨Ã£â‚¬â€š", s.dmPoolKeyboard())
		}
		result, err := s.dmManager.StartDMLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÂ¯Ã¥Å Â¨Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§â„¢Â»Ã¥Â½â€¢Ã¥Â¤Â±Ã¨Â´Â¥: "+err.Error(), s.dmPoolKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.dmPoolText(), s.dmPoolKeyboard())
	case pendingAddKeywordsExact:
		added, err := s.keywordStore.AddWithMode("exact", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã¦Â·Â»Ã¥Å Â  %d Ã¤Â¸ÂªÃ§Â²Â¾Ã¥â€¡â€ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ£â‚¬â€š\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingAddKeywordsFuzzy, pendingAddKeywords:
		added, err := s.keywordStore.AddWithMode("fuzzy", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã¦Â·Â»Ã¥Å Â  %d Ã¤Â¸ÂªÃ¦Â¨Â¡Ã§Â³Å Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ£â‚¬â€š\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingRemoveKeyword:
		removed, err := s.keywordStore.Remove(splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã¥Ë†Â Ã©â„¢Â¤ %d Ã¤Â¸ÂªÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ£â‚¬â€š\n\n%s", removed, s.keywordsText()), s.keywordsKeyboard())
	case pendingSetCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÅ’Ã§â€Â¨Ã¦Ë†Â·Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã¦Â­Â£Ã¦â€¢Â´Ã¦â€¢Â°Ã£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÅ’Ã§â€Â¨Ã¦Ë†Â·Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetChatCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÅ’Ã§Â¾Â¤Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã©ÂÅ¾Ã¨Â´Å¸Ã¦â€¢Â´Ã¦â€¢Â°Ã£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetChatCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÅ’Ã§Â¾Â¤Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTextCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÅ’Ã¥â€ â€¦Ã¥Â®Â¹Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã©ÂÅ¾Ã¨Â´Å¸Ã¦â€¢Â´Ã¦â€¢Â°Ã£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetTextCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥ÂÅ’Ã¥â€ â€¦Ã¥Â®Â¹Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTemplate:
		if strings.TrimSpace(text) == "" {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Â¨Â¡Ã¦ÂÂ¿Ã¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©ÂºÃ£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetDMTemplate(text); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã§Â§ÂÃ¤Â¿Â¡Ã¦Â¨Â¡Ã¦ÂÂ¿Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddDMTemplate:
		templates, parseErr := parseDMTemplateInputs(text)
		if parseErr != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, parseErr.Error()+"\n\n"+s.dmTemplateAddPrompt(), s.dmTemplatesKeyboard())
		}
		if len(templates) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æ¨¡æ¿ä¸èƒ½ä¸ºç©ºã€‚", s.dmTemplatesKeyboard())
		}
		added, err := s.settings.AddDMTemplates(templates)
		if err != nil {
			return err
		}
		if added > 0 {
			_ = s.settings.SetDMTemplate(templates[0])
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²æ·»åŠ  %d æ¡è¯æœ¯ã€‚\n\n%s", added, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingAddDMText:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeTextDMTemplate(text), "æ–‡æœ¬ç›´å‘")
	case pendingAddDMPostBot:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodePostBotDMTemplate(text), "å†…è”Bot @PostBot")
	case pendingAddDMForward:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeForwardDMTemplate(text), "é¢‘é“è´´æ–‡è½¬å‘")
	case pendingAddDMHidden:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeHiddenForwardDMTemplate(text), "éšè—è½¬å‘æ¥æº")
	case pendingAddDMQuickReply:
		shortcutID, err := parseNonNegativeInt(text)
		if err != nil || shortcutID <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ä¼ä¸šå¿«æ·å›žå¤ ID å¿…é¡»æ˜¯å¤§äºŽ 0 çš„æ•°å­—ã€‚", s.dmTemplateModeKeyboard())
		}
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeQuickReplyDMTemplate(shortcutID), "ä¼ä¸šå¿«æ·å›žå¤")
	case pendingRemoveDMTpl:
		removedTargets := resolveDMTemplateRemovals(text, s.settings.ListDMTemplates())
		if len(removedTargets) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "æ²¡æœ‰è¯†åˆ«åˆ°å¯åˆ é™¤çš„è¯æœ¯ç¼–å·æˆ–å†…å®¹ã€‚", s.dmTemplatesKeyboard())
		}
		removed, err := s.settings.RemoveDMTemplates(removedTargets)
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("å·²åˆ é™¤ %d æ¡è¯æœ¯ã€‚\n\n%s", removed, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingSetMaxLength:
		maxLength, err := parseNonNegativeInt(text)
		if err != nil || maxLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦Ã¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã©ÂÅ¾Ã¨Â´Å¸Ã¦â€¢Â´Ã¦â€¢Â°Ã£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetMaxMessageLength(maxLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinLength:
		minLength, err := parseNonNegativeInt(text)
		if err != nil || minLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Å“â‚¬Ã¥Â°ÂÃ¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦Ã¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã©ÂÅ¾Ã¨Â´Å¸Ã¦â€¢Â´Ã¦â€¢Â°Ã£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetMinMessageLength(minLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Å“â‚¬Ã¥Â°ÂÃ¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinAge:
		days, err := parseNonNegativeInt(text)
		if err != nil || days < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Å“â‚¬Ã¥Â°ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€žÃ¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã©ÂÅ¾Ã¨Â´Å¸Ã¦â€¢Â´Ã¦â€¢Â°Ã£â‚¬â€š", s.rulesKeyboard())
		}
		if err := s.settings.SetMinAccountAgeDays(days); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€žÃ¨Â¿â€¡Ã¦Â»Â¤Ã¥Â·Â²Ã¦â€ºÂ´Ã¦â€“Â°Ã£â‚¬â€š\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddChatIDs:
		added, err := s.settings.AddMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã¦Â·Â»Ã¥Å Â  %d Ã¤Â¸ÂªÃ§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤Ã£â‚¬â€š\n\n%s", added, s.chatsText()), s.chatsKeyboard())
	case pendingRemoveChatIDs:
		removed, err := s.settings.RemoveMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã§Â§Â»Ã©â„¢Â¤ %d Ã¤Â¸ÂªÃ§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤Ã£â‚¬â€š\n\n%s", removed, s.chatsText()), s.chatsKeyboard())
	case pendingUnblockUsers:
		removed, err := s.removeBlockedUsers(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã§Â§Â»Ã¥â€¡Âº %d Ã¤Â¸ÂªÃ©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§â€Â¨Ã¦Ë†Â·Ã£â‚¬â€š\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingUnblockChats:
		removed, err := s.removeBlockedChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã§Â§Â»Ã¥â€¡Âº %d Ã¤Â¸ÂªÃ©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§Â¾Â¤Ã£â‚¬â€š\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingExportTime:
		start, end, err := parseTimeRange(text)
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦â€”Â¶Ã©â€”Â´Ã¦Â Â¼Ã¥Â¼ÂÃ¤Â¸ÂÃ¥Â¯Â¹Ã£â‚¬â€š\nÃ§Â¤ÂºÃ¤Â¾â€¹: 09-28-12:00 | 09-28-18:30", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByTime,
			start:      start,
			end:        end,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf(
			"Ã¥Â·Â²Ã©â‚¬â€°Ã¦â€¹Â©Ã¦â€”Â¶Ã©â€”Â´Ã¦Â®ÂµÃ¯Â¼Å¡\n%s ~ %s\n\nÃ¨Â¯Â·Ã©â‚¬â€°Ã¦â€¹Â©Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦Â Â¼Ã¥Â¼ÂÃ£â‚¬â€š",
			start.Format("01-02 15:04"),
			end.Format("01-02 15:04"),
		), s.exportFormatKeyboard())
	case pendingExportKeyword:
		keywords := splitInputParts(text)
		if len(keywords) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©ÂºÃ£â‚¬â€š", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByWords,
			keywords:   keywords,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â·Â²Ã©â‚¬â€°Ã¦â€¹Â©Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å¡"+strings.Join(keywords, ", ")+"\n\nÃ¨Â¯Â·Ã©â‚¬â€°Ã¦â€¹Â©Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦Â Â¼Ã¥Â¼ÂÃ£â‚¬â€š", s.exportFormatKeyboard())
	default:
		return nil
	}
}

func (s *AdminService) handlePendingDMUpload(ctx context.Context, msg *model.Message) error {
	if msg.Document == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¨Â¯Â·Ã¥Ââ€˜Ã©â‚¬Â `.session` Ã¦Ë†â€“ `.zip` Ã¦â€“â€¡Ã¤Â»Â¶Ã£â‚¬â€š", s.dmPoolKeyboard())
	}
	if s.dmManager == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã¥ÂÂ¯Ã§â€Â¨Ã§Å¡â€žÃ§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§Â®Â¡Ã§Ââ€ Ã¥â„¢Â¨Ã£â‚¬â€š", s.dmPoolKeyboard())
	}

	filename := strings.TrimSpace(msg.Document.FileName)
	lowerName := strings.ToLower(filename)
	if !strings.HasSuffix(lowerName, ".session") && !strings.HasSuffix(lowerName, ".zip") {
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦â€“â€¡Ã¤Â»Â¶Ã¦Â Â¼Ã¥Â¼ÂÃ¤Â¸ÂÃ¦â€Â¯Ã¦Å’ÂÃ¯Â¼Å’Ã¤Â»â€¦Ã¦â€Â¯Ã¦Å’Â `.session` Ã¦Ë†â€“ `.zip`Ã£â‚¬â€š", s.dmPoolKeyboard())
	}

	downloader, ok := s.client.(fileDownloader)
	if !ok {
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â½â€œÃ¥â€°Â Bot API Ã¥Â®Â¢Ã¦Ë†Â·Ã§Â«Â¯Ã¤Â¸ÂÃ¦â€Â¯Ã¦Å’ÂÃ¤Â¸â€¹Ã¨Â½Â½Ã¦â€“â€¡Ã¤Â»Â¶Ã£â‚¬â€š", s.dmPoolKeyboard())
	}

	if err := s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â¼â‚¬Ã¥Â§â€¹Ã¤Â¸â€¹Ã¨Â½Â½Ã¥Â¹Â¶Ã¥Â¯Â¼Ã¥â€¦Â¥ SessionÃ¯Â¼Å’Ã¦â€¢Â°Ã©â€¡ÂÃ¥Â¤Å¡Ã¦â€”Â¶Ã¤Â¼Å¡Ã§Â¨ÂÃ§Â­â€°Ã¤Â¸â‚¬Ã¤Â¼Å¡Ã¥â€žÂ¿Ã£â‚¬â€š", nil); err != nil {
		return err
	}

	downloadedName, data, err := downloader.DownloadFile(ctx, msg.Document.FileID)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¤Â¸â€¹Ã¨Â½Â½Ã¦â€“â€¡Ã¤Â»Â¶Ã¥Â¤Â±Ã¨Â´Â¥: "+err.Error(), s.dmPoolKeyboard())
	}
	if strings.TrimSpace(filename) == "" {
		filename = downloadedName
	}

	result, err := s.dmManager.ImportDMSessions(ctx, filename, data)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¥Â¯Â¼Ã¥â€¦Â¥ Session Ã¥Â¤Â±Ã¨Â´Â¥: "+err.Error(), s.dmPoolKeyboard())
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
	return fmt.Sprintf("Ã°Å¸â€œÂ± Ã§â€ºâ€˜Ã¦Å½Â§Ã¨Â´Â¦Ã¥ÂÂ·Ã§Â®Â¡Ã§Ââ€ \n\nÃ¥Â·Â²Ã§â„¢Â»Ã¥Â½â€¢Ã¨Â´Â¦Ã¥ÂÂ·: %d\nÃ¥Å“Â¨Ã§ÂºÂ¿: %d | Ã§Â¦Â»Ã§ÂºÂ¿: %d", total, active, total-active)
}

func (s *AdminService) accountAddPrompt() string {
	return "Ã¨Â¯Â·Ã¨Â¾â€œÃ¥â€¦Â¥Ã§â€ºâ€˜Ã¦Å½Â§Ã¨Â´Â¦Ã¥ÂÂ·Ã§Å¡â€žÃ¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã£â‚¬â€š\n\nÃ¦â€Â¯Ã¦Å’ÂÃ¦Â Â¼Ã¥Â¼ÂÃ¯Â¼Å¡\nÃ¢â‚¬Â¢ +8613800138000\nÃ¢â‚¬Â¢ 8613800138000\nÃ¢â‚¬Â¢ +66955305284"
}

func (s *AdminService) accountsListText() string {
	accounts := s.listMonitorAccounts()
	if len(accounts) == 0 {
		return "Ã¢ÂÅ’ Ã¦Å¡â€šÃ¦â€”Â Ã§â€ºâ€˜Ã¦Å½Â§Ã¨Â´Â¦Ã¥ÂÂ·\n\nÃ§â€šÂ¹Ã¥â€¡Â»Ã¢â‚¬Å“Ã¦Â·Â»Ã¥Å Â Ã¦â€“Â°Ã¨Â´Â¦Ã¥ÂÂ·Ã¢â‚¬ÂÃ¥Â¼â‚¬Ã¥Â§â€¹Ã¦Â·Â»Ã¥Å Â Ã£â‚¬â€š"
	}

	lines := []string{fmt.Sprintf("Ã°Å¸â€œâ€¹ Ã¨Â´Â¦Ã¥ÂÂ·Ã¥Ë†â€”Ã¨Â¡Â¨ (%dÃ¤Â¸Âª)Ã¯Â¼Å¡", len(accounts)), ""}
	for i, account := range accounts {
		status := "Ã°Å¸â€Â´ Ã§Â¦Â»Ã§ÂºÂ¿"
		if account.Online {
			status = "Ã°Å¸Å¸Â¢ Ã¥Å“Â¨Ã§ÂºÂ¿"
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
		return "", nil, fmt.Errorf("Ã§â€ºâ€˜Ã¦Å½Â§Ã¥ÂÂ·Ã¤Â¸ÂÃ¥Â­ËœÃ¥Å“Â¨")
	}

	status := "Ã°Å¸â€Â´ Ã§Â¦Â»Ã§ÂºÂ¿"
	if account.Online {
		status = "Ã°Å¸Å¸Â¢ Ã¥Å“Â¨Ã§ÂºÂ¿"
	}
	text := fmt.Sprintf("Ã°Å¸â€œÂ± Ã¨Â´Â¦Ã¥ÂÂ·Ã¨Â¯Â¦Ã¦Æ’â€¦\n\nÃ¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·: %s\nÃ§Å Â¶Ã¦â‚¬Â: %s\nSession: %s", account.Phone, status, filepathBase(account.SessionFile))
	if strings.TrimSpace(account.LastError) != "" {
		text += "\nÃ©â€â„¢Ã¨Â¯Â¯: " + account.LastError
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã°Å¸â€â€ž Ã©â€¡ÂÃ¦â€“Â°Ã¨Â¿Å¾Ã¦Å½Â¥", CallbackData: callbackAccountRetryPrefix + account.Phone},
				{Text: "Ã¢ÂÅ’ Ã¥Ë†Â Ã©â„¢Â¤Ã¨Â´Â¦Ã¥ÂÂ·", CallbackData: callbackAccountDeletePrefix + account.Phone},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¥Ë†â€”Ã¨Â¡Â¨", CallbackData: callbackAccountList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) keywordsText() string {
	keywords := s.keywordStore.ListEntries()
	if len(keywords) == 0 {
		return "Ã°Å¸â€œÂ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Ë†â€”Ã¨Â¡Â¨Ã¤Â¸ÂºÃ§Â©Âº"
	}
	lines := []string{fmt.Sprintf("Ã°Å¸â€œÂ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Ë†â€”Ã¨Â¡Â¨ (%dÃ¤Â¸Âª)Ã¯Â¼Å¡", len(keywords)), ""}
	const previewLimit = 60
	for i, keyword := range keywords {
		if i >= previewLimit {
			lines = append(lines, fmt.Sprintf("Ã¢â‚¬Â¦Ã¢â‚¬Â¦ Ã¨Â¿ËœÃ¦Å“â€° %d Ã¤Â¸ÂªÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¦Å“ÂªÃ¥Â±â€¢Ã¥Â¼â‚¬Ã¦ËœÂ¾Ã§Â¤Âº", len(keywords)-previewLimit))
			break
		}
		modeLabel := "Ã¦Â¨Â¡Ã§Â³Å "
		if strings.EqualFold(strings.TrimSpace(keyword.Mode), "exact") {
			modeLabel = "Ã§Â²Â¾Ã¥â€¡â€ "
		}
		lines = append(lines, fmt.Sprintf("%d. [%s] %s", i+1, modeLabel, keyword.Text))
	}
	lines = append(lines, "", "Ã¦Â¨Â¡Ã§Â³Å Ã¯Â¼Å¡Ã¤Â¸â‚¬Ã¥ÂÂ¥Ã¨Â¯ÂÃ©â€¡Å’Ã¥Å’â€¦Ã¥ÂÂ«Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â°Â±Ã¥â€˜Â½Ã¤Â¸Â­", "Ã§Â²Â¾Ã¥â€¡â€ Ã¯Â¼Å¡Ã¦â€¢Â´Ã¥ÂÂ¥Ã¥â€ â€¦Ã¥Â®Â¹Ã¥Â¿â€¦Ã©Â¡Â»Ã¤Â¸Å½Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â®Å’Ã¥â€¦Â¨Ã¤Â¸â‚¬Ã¨â€¡Â´Ã¦â€°ÂÃ¥â€˜Â½Ã¤Â¸Â­")
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
		"Ã°Å¸â€™Â¬ Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¦Â±Â \n\nÃ¥Â·Â²Ã§â„¢Â»Ã¥Â½â€¢Ã¨Â´Â¦Ã¥ÂÂ·: %d\nÃ¥Å“Â¨Ã§ÂºÂ¿: %d | Ã§Â¦Â»Ã§ÂºÂ¿: %d\nÃ¨Â¯ÂÃ¦Å“Â¯Ã¦Â¨Â¡Ã¦ÂÂ¿: %d Ã¦ÂÂ¡\nÃ¤Â»Å Ã¦â€”Â¥Ã§Â§ÂÃ¤Â¿Â¡: Ã¥Ââ€˜Ã©â‚¬Â %d | Ã¦Ë†ÂÃ¥Å Å¸ %d | Ã¥Â¤Â±Ã¨Â´Â¥ %d\n\nÃ¦â€Â¯Ã¦Å’ÂÃ¤Â¸Â¤Ã§Â§ÂÃ¦Å½Â¥Ã¥â€¦Â¥Ã¦â€“Â¹Ã¥Â¼ÂÃ¯Â¼Å¡\nÃ¢â‚¬Â¢ Ã¦â€°â€¹Ã¥Å Â¨Ã¨Â¾â€œÃ¥â€¦Â¥Ã¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã§â„¢Â»Ã¥Â½â€¢\nÃ¢â‚¬Â¢ Ã¤Â¸Å Ã¤Â¼Â  Telethon `.session` / `.zip` Ã¦â€°Â¹Ã©â€¡ÂÃ¥Â¯Â¼Ã¥â€¦Â¥",
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
	return "Ã¨Â¯Â·Ã¨Â¾â€œÃ¥â€¦Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·Ã£â‚¬â€š\n\nÃ¦â€Â¯Ã¦Å’ÂÃ¦Â Â¼Ã¥Â¼ÂÃ¯Â¼Å¡\nÃ¢â‚¬Â¢ +8613800138000\nÃ¢â‚¬Â¢ 8613800138000\nÃ¢â‚¬Â¢ +66955305284"
}

func (s *AdminService) dmUploadPrompt() string {
	return "Ã¨Â¯Â·Ã¥Ââ€˜Ã©â‚¬Â Telethon `.session` Ã¦â€“â€¡Ã¤Â»Â¶Ã¯Â¼Å’Ã¦Ë†â€“Ã¤Â¸â‚¬Ã¤Â¸ÂªÃ¥Å’â€¦Ã¥ÂÂ«Ã¥Â¤Å¡Ã¤Â¸Âª `.session` Ã§Å¡â€ž `.zip` Ã¥Å½â€¹Ã§Â¼Â©Ã¥Å’â€¦Ã£â‚¬â€š\n\nÃ¤Â¸Å Ã¤Â¼Â Ã¥ÂÅ½Ã¤Â¼Å¡Ã¨â€¡ÂªÃ¥Å Â¨Ã¦â€°Â¹Ã©â€¡ÂÃ¥Â¯Â¼Ã¥â€¦Â¥Ã¥Â¹Â¶Ã¥Â°ÂÃ¨Â¯â€¢Ã¦â€¹â€°Ã¨ÂµÂ·Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã£â‚¬â€š"
}

func (s *AdminService) dmAccountsText() string {
	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return "Ã¢ÂÅ’ Ã¦Å¡â€šÃ¦â€”Â Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·\n\nÃ§â€šÂ¹Ã¥â€¡Â»Ã¢â‚¬Å“Ã¨Â¿Å¾Ã¦Å½Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¢â‚¬ÂÃ¦â€°â€¹Ã¥Å Â¨Ã§â„¢Â»Ã¥Â½â€¢Ã¯Â¼Å’Ã¦Ë†â€“Ã§â€šÂ¹Ã¢â‚¬Å“Ã¤Â¸Å Ã¤Â¼Â  SessionÃ¢â‚¬ÂÃ¦â€°Â¹Ã©â€¡ÂÃ¥Â¯Â¼Ã¥â€¦Â¥Ã£â‚¬â€š"
	}

	lines := []string{fmt.Sprintf("Ã°Å¸â€œâ€¹ Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¥Ë†â€”Ã¨Â¡Â¨ (%dÃ¤Â¸Âª)Ã¯Â¼Å¡", len(accounts)), ""}
	for i, account := range accounts {
		status := "Ã°Å¸â€Â´ Ã§Â¦Â»Ã§ÂºÂ¿"
		if account.Online {
			status = "Ã°Å¸Å¸Â¢ Ã¥Å“Â¨Ã§ÂºÂ¿"
		}
		line := fmt.Sprintf("%d. %s %s | Ã¤Â»Å Ã¦â€”Â¥ %d Ã¦ÂÂ¡", i+1, account.Phone, status, account.TodaySent)
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
		return "", nil, fmt.Errorf("Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¤Â¸ÂÃ¥Â­ËœÃ¥Å“Â¨")
	}

	status := "Ã°Å¸â€Â´ Ã§Â¦Â»Ã§ÂºÂ¿"
	if account.Online {
		status = "Ã°Å¸Å¸Â¢ Ã¥Å“Â¨Ã§ÂºÂ¿"
	}
	text := fmt.Sprintf("Ã°Å¸â€™Â¬ Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¨Â¯Â¦Ã¦Æ’â€¦\n\nÃ¦â€°â€¹Ã¦Å“ÂºÃ¥ÂÂ·: %s\nÃ§Å Â¶Ã¦â‚¬Â: %s\nSession: %s\nÃ¤Â»Å Ã¦â€”Â¥Ã¥Ââ€˜Ã©â‚¬Â: %d Ã¦ÂÂ¡\nÃ¤Â»Å Ã¦â€”Â¥Ã¦Ë†ÂÃ¥Å Å¸: %d Ã¦ÂÂ¡\nÃ¤Â»Å Ã¦â€”Â¥Ã¥Â¤Â±Ã¨Â´Â¥: %d Ã¦ÂÂ¡", account.Phone, status, filepathBase(account.SessionFile), account.TodaySent, account.TodaySuccess, account.TodayFailed)
	if strings.TrimSpace(account.LastError) != "" {
		text += "\nÃ©â€â„¢Ã¨Â¯Â¯: " + account.LastError
	}
	if label := dmStatusLabel(account.StatusCode); label != "" {
		text += "\nÃ¨Â´Â¦Ã¥ÂÂ·Ã©â„¢ÂÃ¥Ë†Â¶: " + label
	}
	if strings.TrimSpace(account.StatusSummary) != "" {
		text += "\nSpamBot Ã¦Â£â‚¬Ã¦Âµâ€¹: " + account.StatusSummary
		if !account.StatusCheckedAt.IsZero() {
			text += "\nÃ¦Â£â‚¬Ã¦Âµâ€¹Ã¦â€”Â¶Ã©â€”Â´: " + account.StatusCheckedAt.Format("2006-01-02 15:04:05")
		}
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã°Å¸â€Å½ Ã¦Â£â‚¬Ã¦Å¸Â¥Ã§Å Â¶Ã¦â‚¬Â", CallbackData: callbackDMCheckPrefix + account.Phone},
				{Text: "Ã°Å¸â€â€ž Ã©â€¡ÂÃ¦â€“Â°Ã¨Â¿Å¾Ã¦Å½Â¥", CallbackData: callbackDMRetryPrefix + account.Phone},
			},
			{
				{Text: "Ã¢ÂÅ’ Ã¥Ë†Â Ã©â„¢Â¤Ã¨Â´Â¦Ã¥ÂÂ·", CallbackData: callbackDMDeletePrefix + account.Phone},
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¥Ë†â€”Ã¨Â¡Â¨", CallbackData: callbackDMList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) dmTemplatesText() string {
	templates := s.settings.ListDMTemplates()
	if len(templates) == 0 {
		return strings.Join([]string{
			"ðŸ“ æš‚æ— ç§ä¿¡è¯æœ¯æ¨¡æ¿",
			"",
			"æ–°å¢žå‘é€æ¨¡å¼ï¼š",
			"ðŸ”´ æ–‡æœ¬ç›´å‘: 0 æ¡",
			"ðŸ”´ å†…è”Bot @PostBot: 0 æ¡",
			"ðŸ”´ é¢‘é“è´´æ–‡è½¬å‘: 0 æ¡",
			"ðŸ”´ éšè—è½¬å‘æ¥æº: 0 æ¡",
			"ðŸ”´ ä¼ä¸šå¿«æ·å›žå¤: 0 æ¡",
		}, "\n")
	}

	counts := countDMTemplateModes(templates)
	lines := []string{
		fmt.Sprintf("ðŸ“ è¯æœ¯æ¨¡æ¿ (%dæ¡)ï¼š", len(templates)),
		"",
		fmt.Sprintf("ðŸ”´ æ–‡æœ¬ç›´å‘: %d æ¡", counts[model.DMTemplateModeText]),
		fmt.Sprintf("ðŸ”´ å†…è”Bot @PostBot: %d æ¡", counts[model.DMTemplateModePostBot]),
		fmt.Sprintf("ðŸ”´ é¢‘é“è´´æ–‡è½¬å‘: %d æ¡", counts[model.DMTemplateModeForward]),
		fmt.Sprintf("ðŸ”´ éšè—è½¬å‘æ¥æº: %d æ¡", counts[model.DMTemplateModeForwardHidden]),
		fmt.Sprintf("ðŸ”´ ä¼ä¸šå¿«æ·å›žå¤: %d æ¡", counts[model.DMTemplateModeQuickReply]),
		"",
	}
	for i, item := range templates {
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, model.ParseDMTemplate(item).Summary()))
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmTemplateAddPrompt() string {
	return strings.Join([]string{
		"å‘é€è¦æ·»åŠ çš„è¯æœ¯ï¼Œå¤šæ¡å¯ç”¨ --- åˆ†éš”ã€‚",
		"",
		"æ”¯æŒè¿™å‡ ç§æ ¼å¼ï¼š",
		"1. æ–‡æœ¬ç›´å‘ï¼šç›´æŽ¥è¾“å…¥å†…å®¹ï¼Œæˆ–ç”¨ æ–‡æœ¬::å†…å®¹",
		"2. å†…è”Botï¼šPostBot::ä»£ç ",
		"3. é¢‘é“è´´æ–‡è½¬å‘ï¼šè½¬å‘::https://t.me/é¢‘é“/123",
		"4. éšè—è½¬å‘æ¥æºï¼šéšè—è½¬å‘::https://t.me/é¢‘é“/123",
		"5. ä¼ä¸šå¿«æ·å›žå¤ï¼šå¿«æ·å›žå¤::123",
	}, "\n")
}

func (s *AdminService) dmTemplateTextPrompt() string {
	return "å‘é€æ–‡æœ¬ç§ä¿¡å†…å®¹ã€‚\n\næ”¯æŒå˜é‡ï¼š{username} {chat_title} {keywords} {message}"
}

func (s *AdminService) dmTemplatePostBotPrompt() string {
	return "å‘é€ PostBot çš„å†…è”ä»£ç ã€‚\n\nä¾‹å¦‚ï¼šabc123"
}

func (s *AdminService) dmTemplateForwardPrompt(hidden bool) string {
	if hidden {
		return "å‘é€è¦éšè—æ¥æºè½¬å‘çš„é¢‘é“è´´æ–‡é“¾æŽ¥ã€‚\n\nä¾‹å¦‚ï¼šhttps://t.me/channelname/123"
	}
	return "å‘é€è¦è½¬å‘çš„é¢‘é“è´´æ–‡é“¾æŽ¥ã€‚\n\nä¾‹å¦‚ï¼šhttps://t.me/channelname/123"
}

func (s *AdminService) dmTemplateQuickReplyPrompt() string {
	return "å‘é€ä¼ä¸šå¿«æ·å›žå¤ IDã€‚\n\nä¾‹å¦‚ï¼š123"
}

func (s *AdminService) handleAddSingleDMTemplate(ctx context.Context, chatID int64, encoded, label string) error {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return s.client.SendMessage(ctx, chatID, "å†…å®¹ä¸èƒ½ä¸ºç©ºã€‚", s.dmTemplateModeKeyboard())
	}
	added, err := s.settings.AddDMTemplates([]string{encoded})
	if err != nil {
		return err
	}
	if added > 0 {
		_ = s.settings.SetDMTemplate(encoded)
	}
	return s.client.SendMessage(ctx, chatID, fmt.Sprintf("å·²æ·»åŠ  %s è¯æœ¯ã€‚\n\n%s", label, s.dmTemplatesText()), s.dmTemplatesKeyboard())
}

func (s *AdminService) dmRecordsText() string {
	records := s.recordStore.DMRecords()
	if len(records) == 0 {
		return "Ã°Å¸â€œÂ¨ Ã¦Å¡â€šÃ¦â€”Â Ã§Â§ÂÃ¤Â¿Â¡Ã¨Â®Â°Ã¥Â½â€¢"
	}

	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	lines := []string{
		fmt.Sprintf("Ã°Å¸â€œÂ¨ Ã§Â§ÂÃ¤Â¿Â¡Ã¨Â®Â°Ã¥Â½â€¢\n\nÃ¤Â»Å Ã¦â€”Â¥Ã¥Ââ€˜Ã©â‚¬Â: %d | Ã¦Ë†ÂÃ¥Å Å¸ %d | Ã¥Â¤Â±Ã¨Â´Â¥ %d", todaySent, todaySuccess, todayFailed),
		"",
		"Ã¦Å“â‚¬Ã¨Â¿â€˜ 10 Ã¦ÂÂ¡Ã¯Â¼Å¡",
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
		line := fmt.Sprintf("Ã¢â‚¬Â¢ %s | %s | via %s", target, record.Status, senderLabel)
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
		"Ã¢Å¡â„¢Ã¯Â¸Â Ã§Â§ÂÃ¤Â¿Â¡Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â®Â¾Ã§Â½Â®\n\nÃ¥â€ Â·Ã¥ÂÂ´Ã¦â€”Â¶Ã©â€”Â´: %d Ã¥Ë†â€ Ã©â€™Å¸\nÃ¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼Â: %s\nÃ¥Â½â€œÃ¥â€°ÂÃ©Â»ËœÃ¨Â®Â¤Ã¦Â¨Â¡Ã¦ÂÂ¿:\n%s",
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		state.DMTemplate,
	)
}

func (s *AdminService) exportText() string {
	total := len(s.recordStore.MatchRecords())
	return fmt.Sprintf(
		"Ã°Å¸â€œÂ¤ Ã¦â€¢Â°Ã¦ÂÂ®Ã¥Â¯Â¼Ã¥â€¡Âº\n\nÃ¥Â½â€œÃ¥â€°ÂÃ¥â€˜Â½Ã¤Â¸Â­Ã¨Â®Â°Ã¥Â½â€¢: %d Ã¦ÂÂ¡\n\nÃ¥ÂÂ¯Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¯Â¼Å¡\nÃ¢â‚¬Â¢ Ã¦Å’â€°Ã¦â€”Â¶Ã©â€”Â´Ã¦Â®ÂµÃ¥Â¯Â¼Ã¥â€¡Âº\nÃ¢â‚¬Â¢ Ã¦Å’â€°Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â¯Â¼Ã¥â€¡Âº\nÃ¢â‚¬Â¢ Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥â€¦Â¨Ã©Æ’Â¨Ã¦â€¢Â°Ã¦ÂÂ®",
		total,
	)
}

func (s *AdminService) exportTimePrompt() string {
	return "Ã°Å¸â€œâ€¦ Ã¦Å’â€°Ã¦â€”Â¶Ã©â€”Â´Ã¦Â®ÂµÃ¥Â¯Â¼Ã¥â€¡Âº\n\nÃ¨Â¯Â·Ã¨Â¾â€œÃ¥â€¦Â¥Ã¦â€”Â¶Ã©â€”Â´Ã¨Å’Æ’Ã¥â€ºÂ´Ã¯Â¼Å¡\nÃ§Â¤ÂºÃ¤Â¾â€¹ 1: 09-28-12:00 | 09-28-18:30\nÃ§Â¤ÂºÃ¤Â¾â€¹ 2: 2026-09-28 12:00 | 2026-09-28 18:30"
}

func (s *AdminService) exportKeywordPrompt() string {
	keywords := s.keywordStore.List()
	return "Ã°Å¸â€â€˜ Ã¦Å’â€°Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â¯Â¼Ã¥â€¡Âº\n\nÃ¥Â½â€œÃ¥â€°ÂÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å¡\n" + strings.Join(keywords, " | ") + "\n\nÃ¨Â¯Â·Ã¨Â¾â€œÃ¥â€¦Â¥Ã¨Â¦ÂÃ¥Â¯Â¼Ã¥â€¡ÂºÃ§Å¡â€žÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¯Â¼Å’Ã¥Â¤Å¡Ã¤Â¸ÂªÃ§â€Â¨ | Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š"
}

func (s *AdminService) legacyRulesText() string {
	state := s.settings.Snapshot()
	alertChat := "Ã¦Å“ÂªÃ¨Â®Â¾Ã§Â½Â®"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"Ã¢Å¡â„¢Ã¯Â¸Â Ã¨Â¿â€¡Ã¦Â»Â¤Ã¨Â®Â¾Ã§Â½Â®\n\nÃ§â€ºâ€˜Ã¦Å½Â§Ã¥Â¼â‚¬Ã¥â€¦Â³: %s\nÃ¥â€ Â·Ã¥ÂÂ´Ã¦â€”Â¶Ã©â€”Â´: %d Ã¥Ë†â€ Ã©â€™Å¸\nÃ¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼Â: %s\nÃ©â‚¬Å¡Ã§Å¸Â¥Ã§Â¾Â¤: %s\nÃ¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦: %s\nÃ¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂ: %s\nÃ¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã¥Â¤Â´Ã¥Æ’Â: %s\nÃ¦Å“â‚¬Ã¥Â°ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€ž: %s Ã¥Â¤Â©\nÃ§Â§ÂÃ¤Â¿Â¡Ã¦Â¨Â¡Ã¦ÂÂ¿:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MaxMessageLength, "Ã¤Â¸ÂÃ©â„¢Â"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "Ã¤Â¸ÂÃ©â„¢Â"),
		state.DMTemplate,
	)
}

func (s *AdminService) rulesText() string {
	state := s.settings.Snapshot()
	alertChat := "Ã¦Å“ÂªÃ¨Â®Â¾Ã§Â½Â®"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"Ã¢Å¡â„¢Ã¯Â¸Â Ã¨Â¿â€¡Ã¦Â»Â¤Ã¨Â®Â¾Ã§Â½Â®\n\nÃ§â€ºâ€˜Ã¦Å½Â§Ã¥Â¼â‚¬Ã¥â€¦Â³: %s\nÃ¥ÂÅ’Ã§â€Â¨Ã¦Ë†Â·Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´: %d Ã¥Ë†â€ Ã©â€™Å¸\nÃ¥ÂÅ’Ã§Â¾Â¤Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´: %s Ã¥Ë†â€ Ã©â€™Å¸\nÃ¥ÂÅ’Ã¥â€ â€¦Ã¥Â®Â¹Ã©â€¡ÂÃ¥Â¤ÂÃ§Â§ÂÃ¤Â¿Â¡Ã¥â€ Â·Ã¥ÂÂ´: %s Ã¥Ë†â€ Ã©â€™Å¸\nÃ¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼Â: %s\nÃ©â‚¬Å¡Ã§Å¸Â¥Ã§Â¾Â¤: %s\nÃ¦Å“â‚¬Ã¥Â°ÂÃ¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦: %s\nÃ¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦: %s\nÃ¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂ: %s\nÃ¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã¥Â¤Â´Ã¥Æ’Â: %s\nÃ¦Å“â‚¬Ã¥Â°ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€ž: %s Ã¥Â¤Â©\nÃ§Â§ÂÃ¤Â¿Â¡Ã¦Â¨Â¡Ã¦ÂÂ¿:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		formatOptionalNumber(state.ChatCooldownMinutes, "Ã¥â€¦Â³Ã©â€”Â­"),
		formatOptionalNumber(state.TextCooldownMinutes, "Ã¥â€¦Â³Ã©â€”Â­"),
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MinMessageLength, "Ã¤Â¸ÂÃ©â„¢ÂÃ¥Ë†Â¶"),
		formatOptionalNumber(state.MaxMessageLength, "Ã¤Â¸ÂÃ©â„¢ÂÃ¥Ë†Â¶"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "Ã¤Â¸ÂÃ©â„¢ÂÃ¥Ë†Â¶"),
		state.DMTemplate,
	)
}

func (s *AdminService) chatsText() string {
	state := s.settings.Snapshot()
	body := "Ã¦Å¡â€šÃ¦â€”Â Ã§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤"
	if len(state.MonitorChatIDs) > 0 {
		parts := make([]string, 0, len(state.MonitorChatIDs))
		for _, id := range state.MonitorChatIDs {
			parts = append(parts, strconv.FormatInt(id, 10))
		}
		body = strings.Join(parts, "\n")
	}
	return fmt.Sprintf("Ã°Å¸â€˜â€š Ã§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤Ã©â€¦ÂÃ§Â½Â®\n\nÃ¥Â½â€œÃ¥â€°ÂÃ¥â€¦Â± %d Ã¤Â¸ÂªÃ¯Â¼Å¡\n%s", len(state.MonitorChatIDs), body)
}

func (s *AdminService) blacklistText() string {
	if s.blacklist == nil {
		return "Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã¦Å“ÂªÃ¥ÂÂ¯Ã§â€Â¨"
	}

	users := s.blacklist.Users()
	userBody := "Ã¦Å¡â€šÃ¦â€”Â Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§â€Â¨Ã¦Ë†Â·"
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
	chatBody := "Ã¦Å¡â€šÃ¦â€”Â Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§Â¾Â¤"
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

	return fmt.Sprintf("Ã°Å¸Å¡Â« Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢\n\nÃ§â€Â¨Ã¦Ë†Â· %d Ã¤Â¸ÂªÃ¯Â¼Å¡\n%s\n\nÃ§Â¾Â¤ %d Ã¤Â¸ÂªÃ¯Â¼Å¡\n%s", len(users), userBody, len(chats), chatBody)
}

func (s *AdminService) statusText() string {
	active, total := 0, 0
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
	}
	matchRecords := s.recordStore.MatchRecords()
	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	return fmt.Sprintf(
		"Ã°Å¸â€œÅ  Ã¨Â¿ÂÃ¨Â¡Å’Ã§Å Â¶Ã¦â‚¬Â\n\nÃ§â€ºâ€˜Ã¦Å½Â§Ã¨Â´Â¦Ã¥ÂÂ·: %dÃ¥Å“Â¨Ã§ÂºÂ¿ / %dÃ§Â¦Â»Ã§ÂºÂ¿\nÃ¥â€¦Â³Ã©â€Â®Ã¨Â¯Â: %dÃ¤Â¸Âª\nÃ¥â€˜Â½Ã¤Â¸Â­Ã¨Â®Â°Ã¥Â½â€¢: %d\nÃ§Â§ÂÃ¤Â¿Â¡Ã¨Â®Â°Ã¥Â½â€¢: Ã¥Ââ€˜Ã©â‚¬Â %d | Ã¦Ë†ÂÃ¥Å Å¸ %d | Ã¥Â¤Â±Ã¨Â´Â¥ %d\nÃ¨Â¿â€¡Ã¦Â»Â¤Ã¥Â¼â‚¬Ã¥â€¦Â³: %s\nÃ¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼Â: %s",
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
				{Text: "Ã¢Å¾â€¢ Ã¦Â·Â»Ã¥Å Â Ã¦â€“Â°Ã¨Â´Â¦Ã¥ÂÂ·", CallbackData: callbackAccountAdd},
				{Text: "Ã°Å¸â€œâ€¹ Ã¨Â´Â¦Ã¥ÂÂ·Ã¥Ë†â€”Ã¨Â¡Â¨", CallbackData: callbackAccountList},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) accountsListKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listMonitorAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+1)
	for _, account := range accounts {
		status := "Ã°Å¸â€Â´"
		if account.Online {
			status = "Ã°Å¸Å¸Â¢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackAccountDetailPrefix + account.Phone},
		})
	}
	rows = append(rows, []model.InlineKeyboardButton{
		{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackAccounts},
	})
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) backToAccountsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackAccounts}},
		},
	}
}

func (s *AdminService) keywordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã¢Å¾â€¢ Ã¦Â¨Â¡Ã§Â³Å Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â", CallbackData: callbackKeywordAdd},
				{Text: "Ã°Å¸Å½Â¯ Ã§Â²Â¾Ã¥â€¡â€ Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â", CallbackData: callbackKeywordAdd + ":exact"},
			},
			{
				{Text: "Ã¢Å¾â€“ Ã¥Ë†Â Ã©â„¢Â¤Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â", CallbackData: callbackKeywordRemove},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmPoolKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã°Å¸â€Å’ Ã¨Â¿Å¾Ã¦Å½Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·", CallbackData: callbackDMConnect},
				{Text: "Ã°Å¸â€œÂ¤ Ã¤Â¸Å Ã¤Â¼Â  Session", CallbackData: callbackDMUpload},
			},
			{
				{Text: "Ã°Å¸â€œâ€¹ Ã¨Â´Â¦Ã¥ÂÂ·Ã¥Ë†â€”Ã¨Â¡Â¨", CallbackData: callbackDMList},
				{Text: "Ã°Å¸â€Â Ã¤Â¸â‚¬Ã©â€Â®Ã¦Â£â‚¬Ã¦Å¸Â¥Ã§Å Â¶Ã¦â‚¬Â", CallbackData: callbackDMCheckAll},
			},
			{
				{Text: "Ã°Å¸â€œÂ Ã¨Â¯ÂÃ¦Å“Â¯Ã¦Â¨Â¡Ã¦ÂÂ¿", CallbackData: callbackDMTemplates},
				{Text: "Ã¢Å¡â„¢Ã¯Â¸Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â®Â¾Ã§Â½Â®", CallbackData: callbackDMSettings},
			},
			{
				{Text: "Ã°Å¸â€œÂ¨ Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â®Â°Ã¥Â½â€¢", CallbackData: callbackDMRecords},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmAccountsKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listDMAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+2)
	for _, account := range accounts {
		status := "Ã°Å¸â€Â´"
		if account.Online {
			status = "Ã°Å¸Å¸Â¢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackDMDetailPrefix + account.Phone},
		})
	}
	rows = append(rows,
		[]model.InlineKeyboardButton{
			{Text: "Ã°Å¸â€Å’ Ã¨Â¿Å¾Ã¦Å½Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·", CallbackData: callbackDMConnect},
			{Text: "Ã°Å¸â€œÂ¤ Ã¤Â¸Å Ã¤Â¼Â  Session", CallbackData: callbackDMUpload},
		},
		[]model.InlineKeyboardButton{{Text: "Ã°Å¸â€Â Ã¤Â¸â‚¬Ã©â€Â®Ã¦Â£â‚¬Ã¦Å¸Â¥Ã§Å Â¶Ã¦â‚¬Â", CallbackData: callbackDMCheckAll}},
		[]model.InlineKeyboardButton{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackDMPool}},
	)
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) dmTemplatesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "âž• æ·»åŠ è¯æœ¯", CallbackData: callbackDMTemplateAdd},
				{Text: "âž– åˆ é™¤è¯æœ¯", CallbackData: callbackDMTemplateRemove},
			},
			{
				{Text: "ðŸ”™ è¿”å›ž", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmTemplateModeKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ðŸ“ æ–‡æœ¬ç›´å‘", CallbackData: callbackDMTemplateAddText},
				{Text: "ðŸ¤– å†…è”Bot", CallbackData: callbackDMTemplateAddPost},
			},
			{
				{Text: "ðŸ“¢ é¢‘é“è½¬å‘", CallbackData: callbackDMTemplateAddFwd},
				{Text: "ðŸ•¶ éšè—æ¥æºè½¬å‘", CallbackData: callbackDMTemplateAddHide},
			},
			{
				{Text: "ðŸ¢ ä¼ä¸šå¿«æ·å›žå¤", CallbackData: callbackDMTemplateAddQuick},
			},
			{
				{Text: "ðŸ”™ è¿”å›žè¯æœ¯åˆ—è¡¨", CallbackData: callbackDMTemplates},
			},
		},
	}
}

func (s *AdminService) dmRecordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã°Å¸â€œÂ¤ Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥Â¼â€šÃ¥Â¸Â¸Ã¥ÂÂ·", CallbackData: callbackDMExportFailed},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmSettingsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã¥Ë†â€¡Ã¦ÂÂ¢ Dry-run", CallbackData: callbackToggleDryRun},
				{Text: "Ã¨Â®Â¾Ã§Â½Â®Ã¥â€ Â·Ã¥ÂÂ´", CallbackData: callbackSetCooldown},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) exportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã°Å¸â€œâ€¦ Ã¦Å’â€°Ã¦â€”Â¶Ã©â€”Â´Ã¦Â®ÂµÃ¥Â¯Â¼Ã¥â€¡Âº", CallbackData: callbackExportByTime}},
			{{Text: "Ã°Å¸â€â€˜ Ã¦Å’â€°Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯ÂÃ¥Â¯Â¼Ã¥â€¡Âº", CallbackData: callbackExportByKeyword}},
			{{Text: "Ã°Å¸â€œâ€¹ Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥â€¦Â¨Ã©Æ’Â¨Ã¦â€¢Â°Ã¦ÂÂ®", CallbackData: callbackExportAll}},
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) exportFormatKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã°Å¸â€˜Â¤ Ã¤Â»â€¦Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂ (TXT)", CallbackData: callbackExportFormatUsers}},
			{{Text: "Ã°Å¸â€ â€ Ã¤Â»â€¦Ã§â€Â¨Ã¦Ë†Â·ID (TXT)", CallbackData: callbackExportFormatIDs}},
			{{Text: "Ã°Å¸â€œÅ  Ã¥Â®Å’Ã¦â€¢Â´Ã¨Â®Â°Ã¥Â½â€¢ (CSV)", CallbackData: callbackExportFormatCSV}},
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) cancelExportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã°Å¸â€â„¢ Ã¥Ââ€“Ã¦Â¶Ë†", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) legacyRulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã¥Â¼â‚¬Ã¥â€¦Â³Ã§â€ºâ€˜Ã¦Å½Â§", CallbackData: callbackToggleMonitor}, {Text: "Ã¥Ë†â€¡Ã¦ÂÂ¢ Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "Ã¨Â®Â¾Ã§Â½Â®Ã¥â€ Â·Ã¥ÂÂ´", CallbackData: callbackSetCooldown}, {Text: "Ã¨Â®Â¾Ã§Â½Â®Ã¦Â¨Â¡Ã¦ÂÂ¿", CallbackData: callbackSetTemplate}},
			{{Text: "Ã¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦", CallbackData: callbackSetMaxLength}, {Text: "Ã¦Å“â‚¬Ã¥Â°ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€ž", CallbackData: callbackSetMinAge}},
			{{Text: "Ã¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂ", CallbackData: callbackToggleNoName}, {Text: "Ã¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã¥Â¤Â´Ã¥Æ’Â", CallbackData: callbackToggleNoPhoto}},
			{{Text: "Ã§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤Ã©â€¦ÂÃ§Â½Â®", CallbackData: callbackChats}, {Text: "Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢", CallbackData: callbackBlacklist}},
			{{Text: "Ã¥Â½â€œÃ¥â€°ÂÃ¨ÂÅ Ã¥Â¤Â©Ã¨Â®Â¾Ã¤Â¸ÂºÃ©â‚¬Å¡Ã§Å¸Â¥Ã§Â¾Â¤", CallbackData: callbackSetAlertChat}},
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) rulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã¥Â¼â‚¬Ã¥â€¦Â³Ã§â€ºâ€˜Ã¦Å½Â§", CallbackData: callbackToggleMonitor}, {Text: "Ã¥Ë†â€¡Ã¦ÂÂ¢ Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "Ã¥ÂÅ’Ã§â€Â¨Ã¦Ë†Â·Ã¥â€ Â·Ã¥ÂÂ´", CallbackData: callbackSetCooldown}, {Text: "Ã¥ÂÅ’Ã§Â¾Â¤Ã¥â€ Â·Ã¥ÂÂ´", CallbackData: callbackSetChatCooldown}},
			{{Text: "Ã¥ÂÅ’Ã¥â€ â€¦Ã¥Â®Â¹Ã¥â€ Â·Ã¥ÂÂ´", CallbackData: callbackSetTextCooldown}, {Text: "Ã¨Â®Â¾Ã§Â½Â®Ã¦Â¨Â¡Ã¦ÂÂ¿", CallbackData: callbackSetTemplate}},
			{{Text: "Ã¦Å“â‚¬Ã¥Â°ÂÃ¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦", CallbackData: callbackSetMinLength}, {Text: "Ã¦Å“â‚¬Ã¥Â¤Â§Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©â€¢Â¿Ã¥ÂºÂ¦", CallbackData: callbackSetMaxLength}},
			{{Text: "Ã¦Å“â‚¬Ã¥Â°ÂÃ¨Â´Â¦Ã¥ÂÂ·Ã¥Â¹Â´Ã©Â¾â€ž", CallbackData: callbackSetMinAge}, {Text: "Ã¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂ", CallbackData: callbackToggleNoName}},
			{{Text: "Ã¨Â¿â€¡Ã¦Â»Â¤Ã¦â€”Â Ã¥Â¤Â´Ã¥Æ’Â", CallbackData: callbackToggleNoPhoto}},
			{{Text: "Ã§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤Ã©â€¦ÂÃ§Â½Â®", CallbackData: callbackChats}, {Text: "Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢", CallbackData: callbackBlacklist}},
			{{Text: "Ã¥Â½â€œÃ¥â€°ÂÃ¨ÂÅ Ã¥Â¤Â©Ã¨Â®Â¾Ã¤Â¸ÂºÃ©â‚¬Å¡Ã§Å¸Â¥Ã§Â¾Â¤", CallbackData: callbackSetAlertChat}},
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) chatsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã¢Å¾â€¢ Ã¦Â·Â»Ã¥Å Â Ã§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤", CallbackData: callbackAddChat}, {Text: "Ã°Å¸â€”â€˜ Ã§Â§Â»Ã©â„¢Â¤Ã§â€ºâ€˜Ã¥ÂÂ¬Ã§Â¾Â¤", CallbackData: callbackRemoveChat}},
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) blacklistKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã§Â§Â»Ã¥â€¡ÂºÃ©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§â€Â¨Ã¦Ë†Â·", CallbackData: callbackUnblockUser}, {Text: "Ã§Â§Â»Ã¥â€¡ÂºÃ©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã§Â¾Â¤", CallbackData: callbackUnblockChat}},
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) statusKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¤Â¸Â»Ã¨ÂÅ“Ã¥Ââ€¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) blockUserCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã¦Å“ÂªÃ¥ÂÂ¯Ã§â€Â¨", nil
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
		return "Ã§â€Â¨Ã¦Ë†Â·Ã¥Â·Â²Ã¥Å“Â¨Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢", nil
	}
	return "Ã¥Â·Â²Ã¦â€¹â€°Ã©Â»â€˜Ã§â€Â¨Ã¦Ë†Â·", nil
}

func (s *AdminService) blockChatCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢Ã¦Å“ÂªÃ¥ÂÂ¯Ã§â€Â¨", nil
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
		return "Ã§Â¾Â¤Ã¥Â·Â²Ã¥Å“Â¨Ã©Â»â€˜Ã¥ÂÂÃ¥Ââ€¢", nil
	}
	return "Ã¥Â·Â²Ã¦â€¹â€°Ã©Â»â€˜Ã§Â¾Â¤", nil
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
		return fmt.Errorf("Ã¨Â¯Â·Ã¥â€¦Ë†Ã©â‚¬â€°Ã¦â€¹Â©Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦ÂÂ¡Ã¤Â»Â¶")
	}

	records := s.filterMatchRecords(exportCtx)
	if len(records) == 0 {
		return fmt.Errorf("Ã¦Â²Â¡Ã¦Å“â€°Ã¥Å’Â¹Ã©â€¦ÂÃ¥Ë†Â°Ã¥ÂÂ¯Ã¥Â¯Â¼Ã¥â€¡ÂºÃ§Å¡â€žÃ¦â€¢Â°Ã¦ÂÂ®")
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
		return fmt.Errorf("Ã¦Å“ÂªÃ§Å¸Â¥Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦Â Â¼Ã¥Â¼Â")
	}
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("Ã¥Â¯Â¼Ã¥â€¡ÂºÃ§Â»â€œÃ¦Å¾Å“Ã¤Â¸ÂºÃ§Â©Âº")
	}

	caption := fmt.Sprintf("Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥Â®Å’Ã¦Ë†ÂÃ¯Â¼Å’Ã¥â€¦Â± %d Ã¦ÂÂ¡Ã¨Â®Â°Ã¥Â½â€¢Ã£â‚¬â€š", len(records))
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
	if err := writer.Write([]string{"Ã§â€Â¨Ã¦Ë†Â·ID", "Ã§â€Â¨Ã¦Ë†Â·Ã¥ÂÂ", "Ã¦ËœÂµÃ§Â§Â°", "Ã¦ÂÂ¥Ã¦ÂºÂÃ§Â¾Â¤Ã§Â»â€ž", "Ã¨Â§Â¦Ã¥Ââ€˜Ã¥â€¦Â³Ã©â€Â®Ã¨Â¯Â", "Ã¨Â§Â¦Ã¥Ââ€˜Ã¦â€”Â¶Ã©â€”Â´", "Ã¦Â¶Ë†Ã¦ÂÂ¯Ã¥â€ â€¦Ã¥Â®Â¹", "Ã§â€ºâ€˜Ã¦Å½Â§Ã¨Â´Â¦Ã¥ÂÂ·"}); err != nil {
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
		return "", fmt.Errorf("å‘é€æ¨¡å¼å†…å®¹ä¸èƒ½ä¸ºç©º")
	}

	switch mode {
	case "æ–‡æœ¬", "æ–‡æœ¬ç›´å‘", "text":
		return model.EncodeTextDMTemplate(value), nil
	case "postbot", "å†…è”bot", "å†…è”":
		return model.EncodePostBotDMTemplate(value), nil
	case "è½¬å‘", "é¢‘é“è½¬å‘", "é¢‘é“è´´æ–‡è½¬å‘", "forward":
		return model.EncodeForwardDMTemplate(value), nil
	case "éšè—è½¬å‘", "éšè—æ¥æº", "éšè—è½¬å‘æ¥æº", "forward_hidden", "hidden_forward":
		return model.EncodeHiddenForwardDMTemplate(value), nil
	case "å¿«æ·å›žå¤", "ä¼ä¸šå¿«æ·å›žå¤", "quick_reply", "quickreply":
		shortcutID, err := strconv.Atoi(value)
		if err != nil || shortcutID <= 0 {
			return "", fmt.Errorf("ä¼ä¸šå¿«æ·å›žå¤æ ¼å¼ä¸å¯¹ï¼Œè¯·ç”¨ å¿«æ·å›žå¤::123 è¿™ç§æ ¼å¼")
		}
		return model.EncodeQuickReplyDMTemplate(shortcutID), nil
	default:
		return "", fmt.Errorf("ä¸æ”¯æŒçš„å‘é€æ¨¡å¼ï¼š%s", strings.TrimSpace(parts[0]))
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
		return "Ã¥Â¼â‚¬Ã¥ÂÂ¯"
	}
	return "Ã¥â€¦Â³Ã©â€”Â­"
}

func formatOptionalNumber(value int, disabled string) string {
	if value <= 0 {
		return disabled
	}
	return strconv.Itoa(value)
}

func dryRunLabel(enabled bool) string {
	if enabled {
		return "Ã¦Â¼â€Ã§Â»Æ’Ã¦Â¨Â¡Ã¥Â¼ÂÃ¯Â¼Ë†Ã¤Â¸ÂÃ§Å“Å¸Ã¥Â®Å¾Ã§Â§ÂÃ¤Â¿Â¡Ã¯Â¼â€°"
	}
	return "Ã§Å“Å¸Ã¥Â®Å¾Ã¥Ââ€˜Ã©â‚¬Â"
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
	return string(runes[:limit-12]) + "\n\nÃ¢â‚¬Â¦Ã¢â‚¬Â¦ Ã¥â€ â€¦Ã¥Â®Â¹Ã¨Â¿â€¡Ã©â€¢Â¿Ã¯Â¼Å’Ã¥Â·Â²Ã¦Ë†ÂªÃ¦â€“Â­"
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
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã¥ÂÂ¯Ã§â€Â¨Ã§Å¡â€žÃ§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§Â®Â¡Ã§Ââ€ Ã¥â„¢Â¨")
	}

	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¥ÂÂ¯Ã¦Â£â‚¬Ã¦Å¸Â¥")
	}

	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¥Â¼â‚¬Ã¥Â§â€¹Ã¦â€°Â¹Ã©â€¡ÂÃ¦Â£â‚¬Ã¦Å¸Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§Å Â¶Ã¦â‚¬Â"); err != nil {
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
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¥Â¼â‚¬Ã¥Â§â€¹Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "Ã¢Å“â€¦ Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¯Â¼Å’Ã¦â€°â‚¬Ã¦Å“â€°Ã¨Â´Â¦Ã¥ÂÂ·Ã©Æ’Â½Ã¥Â±Å¾Ã¤ÂºÅ½Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶Ã§Å Â¶Ã¦â‚¬ÂÃ£â‚¬â€š", s.dmCheckActionsKeyboard())
	}

	if err := s.sendDMAccountExportDocuments(ctx, callback.Message.Chat.ID, accounts, "Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·"); err != nil {
		return err
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("Ã°Å¸â€œÂ¤ Ã¥Â¼â€šÃ¥Â¸Â¸Ã¨Â´Â¦Ã¥ÂÂ·Ã¥Â·Â²Ã¥Â¯Â¼Ã¥â€¡Âº\n\nÃ¥Â·Â²Ã¥Â¯Â¼Ã¥â€¡Âº: %d Ã¤Â¸Âª\nÃ¥Â·Â²Ã¥Ë†Â Ã©â„¢Â¤: %d Ã¤Â¸Âª\nÃ¥Ë†Â Ã©â„¢Â¤Ã¥Â¤Â±Ã¨Â´Â¥: %d Ã¤Â¸Âª\n\nÃ¥Â·Â²Ã¤Â¿ÂÃ§â€¢â„¢: Ã¤Â»â€¦Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶Ã¨Â´Â¦Ã¥ÂÂ·", len(accounts), removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) handleDMKeepOnlyNormal(ctx context.Context, callback *model.CallbackQuery) error {
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "Ã¥Â¼â‚¬Ã¥Â§â€¹Ã¦Â¸â€¦Ã§Ââ€ Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "Ã¢Å“â€¦ Ã¥Â½â€œÃ¥â€°ÂÃ¦Â²Â¡Ã¦Å“â€°Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¯Â¼Å’Ã¥Â·Â²Ã§Â»ÂÃ¥ÂÂªÃ¤Â¿ÂÃ§â€¢â„¢Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶Ã¨Â´Â¦Ã¥ÂÂ·Ã£â‚¬â€š", s.dmAccountsKeyboard())
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("Ã°Å¸Â§Â¹ Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¦Â±Â Ã¥Â·Â²Ã¦Â¸â€¦Ã§Ââ€ \n\nÃ¥Â·Â²Ã¥Ë†Â Ã©â„¢Â¤Ã¥Â¼â€šÃ¥Â¸Â¸Ã¨Â´Â¦Ã¥ÂÂ·: %d Ã¤Â¸Âª\nÃ¥Ë†Â Ã©â„¢Â¤Ã¥Â¤Â±Ã¨Â´Â¥: %d Ã¤Â¸Âª\nÃ¥Â½â€œÃ¥â€°ÂÃ¥ÂÂªÃ¤Â¿ÂÃ§â€¢â„¢Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶Ã¨Â´Â¦Ã¥ÂÂ·Ã£â‚¬â€š", removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) dmCheckProgressText(done, total int, counts map[string]int) string {
	return fmt.Sprintf(
		"Ã°Å¸â€Â Ã¦Â­Â£Ã¥Å“Â¨Ã¦Â£â‚¬Ã¦Å¸Â¥Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§Å Â¶Ã¦â‚¬Â (%d/%d)\n\n%s",
		done,
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckResultText(total int, counts map[string]int) string {
	return fmt.Sprintf(
		"Ã¢Å“â€¦ Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã§Å Â¶Ã¦â‚¬ÂÃ¦Â£â‚¬Ã¦Å¸Â¥Ã¥Â®Å’Ã¦Ë†Â\n\nÃ¦â‚¬Â»Ã¨Â®Â¡: %d Ã¤Â¸ÂªÃ¨Â´Â¦Ã¥ÂÂ·\n%s\nÃ¢Å¡Â Ã¯Â¸Â Ã¦Å½Â¥Ã¤Â¸â€¹Ã¦ÂÂ¥Ã¤Â½Â Ã¥ÂÂ¯Ã¤Â»Â¥Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥Â¼â€šÃ¥Â¸Â¸Ã¨Â´Â¦Ã¥ÂÂ·Ã¯Â¼Å’Ã¦Ë†â€“Ã¨â‚¬â€¦Ã§â€ºÂ´Ã¦Å½Â¥Ã¥ÂÂªÃ¤Â¿ÂÃ§â€¢â„¢Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶Ã¨Â´Â¦Ã¥ÂÂ·Ã£â‚¬â€š",
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckActionsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã°Å¸â€œÂ¤ Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¥Â¼â€šÃ¥Â¸Â¸Ã¥Â¹Â¶Ã¥Ë†Â Ã©â„¢Â¤", CallbackData: callbackDMExportAbnormal},
				{Text: "Ã°Å¸Â§Â¹ Ã¤Â»â€¦Ã¤Â¿ÂÃ§â€¢â„¢Ã¦Â­Â£Ã¥Â¸Â¸Ã¨Â´Â¦Ã¥ÂÂ·", CallbackData: callbackDMKeepOnlyNormal},
			},
			{
				{Text: "Ã°Å¸â€œâ€¹ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¨Â´Â¦Ã¥ÂÂ·Ã¥Ë†â€”Ã¨Â¡Â¨", CallbackData: callbackDMList},
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã§Â§ÂÃ¤Â¿Â¡Ã¥ÂÂ·Ã¦Â±Â ", CallbackData: callbackDMPool},
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
		caption := fmt.Sprintf("Ã°Å¸â€œÂ¦ %s Session Ã¦â€°â€œÃ¥Å’â€¦Ã¯Â¼Ë†%d Ã¤Â¸ÂªÃ¯Â¼â€°", label, sessionCount)
		if err := s.client.SendDocument(ctx, chatID, filename, zipData, caption); err != nil {
			return err
		}
	}

	reportData := buildDMAccountReport(accounts, label)
	reportName := fmt.Sprintf("dm_abnormal_accounts_%s.txt", timestamp)
	reportCaption := fmt.Sprintf("Ã°Å¸â€œâ€¹ %sÃ¥Ë†â€”Ã¨Â¡Â¨Ã¯Â¼Ë†%d Ã¤Â¸ÂªÃ¯Â¼â€°", label, len(accounts))
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
		fmt.Sprintf("# Ã¥Â¯Â¼Ã¥â€¡ÂºÃ¦â€”Â¶Ã©â€”Â´: %s", time.Now().Format("2006-01-02 15:04:05")),
		fmt.Sprintf("# Ã¥â€¦Â± %d Ã¤Â¸ÂªÃ¨Â´Â¦Ã¥ÂÂ·", len(accounts)),
		"",
	}

	for _, account := range accounts {
		code := effectiveDMStatusCode(account)
		line := fmt.Sprintf(
			"%s | %s | Ã¤Â»Å Ã¦â€”Â¥Ã¥Ââ€˜Ã©â‚¬Â %d Ã¦ÂÂ¡ | Ã¤Â»Å Ã¦â€”Â¥Ã¦Ë†ÂÃ¥Å Å¸ %d Ã¦ÂÂ¡ | Ã¤Â»Å Ã¦â€”Â¥Ã¥Â¤Â±Ã¨Â´Â¥ %d Ã¦ÂÂ¡",
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
		fmt.Sprintf("Ã¢Å“â€¦ Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶: %d", counts["active"]),
		fmt.Sprintf("Ã¢Å¡Â Ã¯Â¸Â Ã¤Â¸Â´Ã¦â€”Â¶Ã©â„¢ÂÃ¥Ë†Â¶ / Ã¥ÂÅ’Ã¥Ââ€˜Ã©â„¢ÂÃ¥Ë†Â¶: %d", counts["restricted"]),
		fmt.Sprintf("Ã°Å¸â€œÂµ Ã¥Å¾Æ’Ã¥Å“Â¾Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©Â£Å½Ã¦Å½Â§: %d", counts["spam"]),
		fmt.Sprintf("Ã°Å¸Å¡Â« Ã¥Â°ÂÃ§Â¦ÂÃ¨Â´Â¦Ã¥ÂÂ·: %d", counts["banned"]),
		fmt.Sprintf("Ã¢Ââ€žÃ¯Â¸Â Ã¥â€ Â»Ã§Â»â€œ / Ã¥Â®Â¡Ã¦Â Â¸Ã¤Â¸Â­: %d", counts["frozen"]),
		fmt.Sprintf("Ã°Å¸â€Å’ Ã§Â¦Â»Ã§ÂºÂ¿ / Ã¦Â£â‚¬Ã¦Å¸Â¥Ã¥Â¤Â±Ã¨Â´Â¥: %d", counts["failed"]),
	}
	if counts["unknown"] > 0 {
		lines = append(lines, fmt.Sprintf("Ã¢Ââ€œ Ã¦Å“ÂªÃ¨Â¯â€ Ã¥Ë†Â«Ã§Å Â¶Ã¦â‚¬Â: %d", counts["unknown"]))
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
	case strings.Contains(summary, "Ã¦Â­Â£Ã¥Â¸Â¸"), strings.Contains(summary, "Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶"):
		return "active"
	case strings.Contains(summary, "Ã¥ÂÅ’Ã¥Ââ€˜"), strings.Contains(summary, "Ã¤Â¸Â´Ã¦â€”Â¶Ã©â„¢ÂÃ¥Ë†Â¶"):
		return "restricted"
	case strings.Contains(summary, "Ã©Â£Å½Ã¦Å½Â§"), strings.Contains(summary, "Ã¥Å¾Æ’Ã¥Å“Â¾Ã¦Â¶Ë†Ã¦ÂÂ¯"):
		return "spam"
	case strings.Contains(summary, "Ã¥Â°ÂÃ§Â¦Â"), strings.Contains(summary, "Ã¦Â°Â¸Ã¤Â¹â€¦Ã©â„¢ÂÃ¥Ë†Â¶"):
		return "banned"
	case strings.Contains(summary, "Ã¥Â®Â¡Ã¦Â Â¸"), strings.Contains(summary, "Ã©ÂªÅ’Ã¨Â¯Â"), strings.Contains(summary, "Ã¥â€ Â»Ã§Â»â€œ"):
		return "frozen"
	case strings.Contains(summary, "Ã¥Â¤Â±Ã¨Â´Â¥"), strings.Contains(summary, "Ã§Â¦Â»Ã§ÂºÂ¿"):
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
		return "Ã¢Å“â€¦ Ã¦Â­Â£Ã¥Â¸Â¸Ã¦â€”Â Ã©â„¢ÂÃ¥Ë†Â¶"
	case "restricted":
		return "Ã¢Å¡Â Ã¯Â¸Â Ã¤Â¸Â´Ã¦â€”Â¶Ã©â„¢ÂÃ¥Ë†Â¶ / Ã¥ÂÅ’Ã¥Ââ€˜Ã©â„¢ÂÃ¥Ë†Â¶"
	case "spam":
		return "Ã°Å¸â€œÂµ Ã¥Å¾Æ’Ã¥Å“Â¾Ã¦Â¶Ë†Ã¦ÂÂ¯Ã©Â£Å½Ã¦Å½Â§"
	case "banned":
		return "Ã°Å¸Å¡Â« Ã¥Â°ÂÃ§Â¦Â"
	case "frozen":
		return "Ã¢Ââ€žÃ¯Â¸Â Ã¥â€ Â»Ã§Â»â€œ / Ã¥Â®Â¡Ã¦Â Â¸Ã¤Â¸Â­"
	case "failed":
		return "Ã°Å¸â€Å’ Ã§Â¦Â»Ã§ÂºÂ¿ / Ã¦Â£â‚¬Ã¦Å¸Â¥Ã¥Â¤Â±Ã¨Â´Â¥"
	default:
		return "Ã¢Ââ€œ Ã¦Å“ÂªÃ¨Â¯â€ Ã¥Ë†Â«"
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
		return fmt.Errorf("Ã¦Â²Â¡Ã¦Å“â€°Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¨Â®Â°Ã¥Â½â€¢")
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
	return s.client.SendDocument(ctx, chatID, filename, []byte(strings.Join(lines, "\n")), fmt.Sprintf("Ã¥Â¼â€šÃ¥Â¸Â¸Ã§Â§ÂÃ¤Â¿Â¡Ã¨Â®Â°Ã¥Â½â€¢ %d Ã¦ÂÂ¡", len(failed)))
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

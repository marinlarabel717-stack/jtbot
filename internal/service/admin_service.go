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
			return true, s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â `.session` ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œ `.zip` ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
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
		return true, s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¨Ã‚ÂÃ…Â ÃƒÂ¥Ã‚Â¤Ã‚Â© ID: %d", msg.Chat.ID), nil)
	}

	if s.authInput != nil && !strings.HasPrefix(text, "/") {
		if handled, kind := s.authInput.Submit(text); handled {
			ack := "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂÃ‚Â¶ÃƒÂ¥Ã‹â€ Ã‚Â°ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ©Ã‚ÂªÃ…â€™ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ§Ã‚Â Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ§Ã‚Â»Ã‚Â§ÃƒÂ§Ã‚Â»Ã‚Â­ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡"
			if kind == "password" {
				ack = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂÃ‚Â¶ÃƒÂ¥Ã‹â€ Ã‚Â°ÃƒÂ¤Ã‚Â¸Ã‚Â¤ÃƒÂ¦Ã‚Â­Ã‚Â¥ÃƒÂ©Ã‚ÂªÃ…â€™ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¥Ã‚Â¯Ã¢â‚¬Â ÃƒÂ§Ã‚Â Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ§Ã‚Â»Ã‚Â§ÃƒÂ§Ã‚Â»Ã‚Â­ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡"
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
		return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ¤Ã‚Â¸Ã…Â ÃƒÂ¤Ã‚Â¸Ã¢â‚¬Â¹ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡")
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
		alert = "ÃƒÂ¦Ã…Â Ã…Â ÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Â¹ÃƒÂ¦Ã…â€œÃ‚ÂºÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ§Ã‚Â»Ã¢â€žÂ¢ÃƒÂ¦Ã‹â€ Ã¢â‚¬Ëœ"
	case callbackAccountList:
		text, keyboard = s.accountsListText(), s.accountsListKeyboard()
	case callbackKeywords:
		text, keyboard = s.keywordsText(), s.keywordsKeyboard()
	case callbackKeywordAdd:
		s.setPending(callback.From.ID, pendingAddKeywordsFuzzy)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\nÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¥Ã‚ÂÃ‚Â¥ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ©Ã¢â‚¬Â¡Ã…â€™ÃƒÂ¥Ã‚ÂÃ‚ÂªÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¥Ã…â€™Ã¢â‚¬Â¦ÃƒÂ¥Ã‚ÂÃ‚Â«ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¥Ã‚Â°Ã‚Â±ÃƒÂ¥Ã¢â‚¬ËœÃ‚Â½ÃƒÂ¤Ã‚Â¸Ã‚Â­ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.keywordsKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â"
	case callbackKeywordAdd + ":exact":
		s.setPending(callback.From.ID, pendingAddKeywordsExact)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\nÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…Â¡ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¤Ã‚Â¸Ã…Â½ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¥Ã‚Â®Ã…â€™ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¨ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¨Ã¢â‚¬Â¡Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â°Ã‚ÂÃƒÂ¥Ã¢â‚¬ËœÃ‚Â½ÃƒÂ¤Ã‚Â¸Ã‚Â­ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.keywordsKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â"
	case callbackKeywordRemove:
		s.setPending(callback.From.ID, pendingRemoveKeyword)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\nÃƒÂ¦Ã¢â‚¬ÂÃ‚Â¯ÃƒÂ¦Ã…â€™Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…Â¡\nÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â :ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â\nÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â :ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â\nÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ§Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¦Ã…â€œÃ‚Â¬ÃƒÂ¯Ã‚Â¼Ã‹â€ ÃƒÂ¤Ã‚Â¼Ã…Â¡ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â /ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â ÃƒÂ¨Ã‚Â§Ã¢â‚¬Å¾ÃƒÂ¥Ã‹â€ Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¼Ã¢â‚¬Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.keywordsKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â"
	case callbackDMPool:
		text, keyboard = s.dmPoolText(), s.dmPoolKeyboard()
	case callbackDMConnect:
		s.setPending(callback.From.ID, pendingLoginDM)
		text, keyboard = s.dmConnectPrompt(), s.dmPoolKeyboard()
		alert = "ÃƒÂ¦Ã…Â Ã…Â ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Â¹ÃƒÂ¦Ã…â€œÃ‚ÂºÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ§Ã‚Â»Ã¢â€žÂ¢ÃƒÂ¦Ã‹â€ Ã¢â‚¬Ëœ"
	case callbackDMUpload:
		s.setPending(callback.From.ID, pendingUploadDMSess)
		text, keyboard = s.dmUploadPrompt(), s.dmPoolKeyboard()
		alert = "ÃƒÂ¦Ã…Â Ã…Â  session ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ§Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ§Ã‚Â»Ã¢â€žÂ¢ÃƒÂ¦Ã‹â€ Ã¢â‚¬Ëœ"
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
		text, keyboard = "Ã©â‚¬â€°Ã¦â€¹Â©Ã¨Â¦ÂÃ¦Â·Â»Ã¥Å Â Ã§Å¡â€žÃ¨Â¯ÂÃ¦Å“Â¯Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼ÂÃ£â‚¬â€š", s.dmTemplateModeKeyboard()
		alert = "Ã¨Â¯Â·Ã©â‚¬â€°Ã¦â€¹Â©Ã¨Â¯ÂÃ¦Å“Â¯Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼Â"
	case callbackDMTemplateAddText:
		s.setPending(callback.From.ID, pendingAddDMText)
		text, keyboard = s.dmTemplateTextPrompt(), s.dmTemplateModeKeyboard()
		alert = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦â€“â€¡Ã¦Å“Â¬Ã¨Â¯ÂÃ¦Å“Â¯Ã¥â€ â€¦Ã¥Â®Â¹"
	case callbackDMTemplateAddPost:
		s.setPending(callback.From.ID, pendingAddDMPostBot)
		text, keyboard = s.dmTemplatePostBotPrompt(), s.dmTemplateModeKeyboard()
		alert = "Ã¥Ââ€˜Ã©â‚¬Â PostBot Ã¤Â»Â£Ã§Â Â"
	case callbackDMTemplateAddFwd:
		s.setPending(callback.From.ID, pendingAddDMForward)
		text, keyboard = s.dmTemplateForwardPrompt(false), s.dmTemplateModeKeyboard()
		alert = "Ã¥Ââ€˜Ã©â‚¬ÂÃ©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã©â€œÂ¾Ã¦Å½Â¥"
	case callbackDMTemplateAddHide:
		s.setPending(callback.From.ID, pendingAddDMHidden)
		text, keyboard = s.dmTemplateForwardPrompt(true), s.dmTemplateModeKeyboard()
		alert = "Ã¥Ââ€˜Ã©â‚¬ÂÃ©Å¡ÂÃ¨â€”ÂÃ¦ÂÂ¥Ã¦ÂºÂÃ¨Â½Â¬Ã¥Ââ€˜Ã©â€œÂ¾Ã¦Å½Â¥"
	case callbackDMTemplateAddQuick:
		s.setPending(callback.From.ID, pendingAddDMQuickReply)
		text, keyboard = s.dmTemplateQuickReplyPrompt(), s.dmTemplateModeKeyboard()
		alert = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â ID"
	case callbackDMTemplateRemove:
		s.setPending(callback.From.ID, pendingRemoveDMTpl)
		text, keyboard = "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¥Ë†Â Ã©â„¢Â¤Ã§Å¡â€žÃ¨Â¯ÂÃ¦Å“Â¯Ã§Â¼â€“Ã¥ÂÂ·Ã¦Ë†â€“Ã¥â€ â€¦Ã¥Â®Â¹Ã¯Â¼Å’Ã¦â€Â¯Ã¦Å’ÂÃ¤Â¸â‚¬Ã¦Â¬Â¡Ã¥Ë†Â Ã©â„¢Â¤Ã¥Â¤Å¡Ã¦ÂÂ¡Ã¯Â¼Å’Ã¤Â½Â¿Ã§â€Â¨ |Ã£â‚¬ÂÃ¦ÂÂ¢Ã¨Â¡Å’Ã¦Ë†â€“Ã©â‚¬â€”Ã¥ÂÂ·Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š", s.dmTemplatesKeyboard()
		alert = "Ã§Â­â€°Ã¥Â¾â€¦Ã¤Â½Â Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¥Ë†Â Ã©â„¢Â¤Ã§Å¡â€žÃ¨Â¯ÂÃ¦Å“Â¯"
	case callbackDMRecords:
		text, keyboard = s.dmRecordsText(), s.dmRecordsKeyboard()
	case callbackDMExportFailed:
		err = s.sendFailedDMExport(ctx, callback.Message.Chat.ID)
		alert = "ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº"
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
		text, keyboard = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¨ÃƒÂ©Ã†â€™Ã‚Â¨ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¦Ã‚ÂÃ‚Â®ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã‚Â Ã‚Â¼ÃƒÂ¥Ã‚Â¼Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.exportFormatKeyboard()
	case callbackExportFormatUsers, callbackExportFormatIDs, callbackExportFormatCSV:
		err = s.sendExportFile(ctx, callback.Message.Chat.ID, callback.From.ID, callback.Data)
		alert = "ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â"
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
				alert = "ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚ÂÃ‚Â¯"
			} else {
				alert = "ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­"
			}
		}
	case callbackToggleDryRun:
		var enabled bool
		enabled, err = s.settings.ToggleDryRun()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â¡ÃƒÂ¥Ã‹â€ Ã‚Â° dry-run"
			} else {
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â¡ÃƒÂ¥Ã‹â€ Ã‚Â°ÃƒÂ§Ã…â€œÃ…Â¸ÃƒÂ¥Ã‚Â®Ã…Â¾ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â"
			}
		}
	case callbackSetCooldown:
		s.setPending(callback.From.ID, pendingSetCooldown)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã‚Â¯Ã¢â‚¬ÂÃƒÂ¥Ã‚Â¦Ã¢â‚¬Å¡ 1440ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°"
	case callbackSetChatCooldown:
		s.setPending(callback.From.ID, pendingSetChatCooldown)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¡Ã‚Â« 0 ÃƒÂ¨Ã‚Â¡Ã‚Â¨ÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°"
	case callbackSetTextCooldown:
		s.setPending(callback.From.ID, pendingSetTextCooldown)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¡Ã‚Â« 0 ÃƒÂ¨Ã‚Â¡Ã‚Â¨ÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°"
	case callbackSetTemplate:
		s.setPending(callback.From.ID, pendingSetTemplate)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¥Ã‚ÂÃ‹Å“ÃƒÂ©Ã¢â‚¬Â¡Ã‚Â: {username} {chat_title} {keywords} {message}", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿"
	case callbackSetMinLength:
		s.setPending(callback.From.ID, pendingSetMinLength)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¡Ã‚Â« 0 ÃƒÂ¨Ã‚Â¡Ã‚Â¨ÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦"
	case callbackSetMaxLength:
		s.setPending(callback.From.ID, pendingSetMaxLength)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¡Ã‚Â« 0 ÃƒÂ¨Ã‚Â¡Ã‚Â¨ÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦"
	case callbackSetMinAge:
		s.setPending(callback.From.ID, pendingSetMinAge)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾ÃƒÂ¥Ã‚Â¤Ã‚Â©ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¡Ã‚Â« 0 ÃƒÂ¨Ã‚Â¡Ã‚Â¨ÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾"
	case callbackToggleNoName:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoUsername()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤"
			} else {
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤"
			}
		}
	case callbackToggleNoPhoto:
		var enabled bool
		enabled, err = s.settings.ToggleFilterNoAvatar()
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			if enabled {
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ¥Ã‚Â¤Ã‚Â´ÃƒÂ¥Ã†â€™Ã‚ÂÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤"
			} else {
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ¥Ã‚Â¤Ã‚Â´ÃƒÂ¥Ã†â€™Ã‚ÂÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤"
			}
		}
	case callbackChats:
		text, keyboard = s.chatsText(), s.chatsKeyboard()
	case callbackAddChat:
		s.setPending(callback.From.ID, pendingAddChatIDs)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ IDÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.chatsKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ§Ã‚Â¾Ã‚Â¤ ID"
	case callbackRemoveChat:
		s.setPending(callback.From.ID, pendingRemoveChatIDs)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ IDÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.chatsKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â¾Ã‚Â¤ ID"
	case callbackSetAlertChat:
		err = s.settings.SetAlertChatID(callback.Message.Chat.ID)
		text, keyboard = s.rulesText(), s.rulesKeyboard()
		if err == nil {
			alert = "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¨Ã‚ÂÃ…Â ÃƒÂ¥Ã‚Â¤Ã‚Â©ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ©Ã¢â€šÂ¬Ã…Â¡ÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â¾Ã‚Â¤"
		}
	case callbackBlacklist:
		text, keyboard = s.blacklistText(), s.blacklistKeyboard()
	case callbackUnblockUser:
		s.setPending(callback.From.ID, pendingUnblockUsers)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â· IDÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.blacklistKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â· ID"
	case callbackUnblockChat:
		s.setPending(callback.From.ID, pendingUnblockChats)
		text, keyboard = "ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â¾Ã‚Â¤ IDÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¦Ã‚ÂÃ‚Â¢ÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.blacklistKeyboard()
		alert = "ÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¥Ã‚Â¾Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ§Ã‚Â¾Ã‚Â¤ ID"
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
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ¨Ã‚ÂµÃ‚Â·ÃƒÂ¨Ã‚Â¿Ã…Â¾ÃƒÂ¦Ã…Â½Ã‚Â¥"
				text, keyboard, _ = s.accountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackAccountDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackAccountDeletePrefix)
			err = s.monitorManager.DeleteMonitor(ctx, phone)
			if err == nil {
				alert = "ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤"
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
				alert = "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ¨Ã‚Â¿Ã…Â¾ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·"
				text, keyboard, _ = s.dmAccountDetailText(phone)
			}
		case strings.HasPrefix(callback.Data, callbackDMDeletePrefix):
			phone := strings.TrimPrefix(callback.Data, callbackDMDeletePrefix)
			err = s.dmManager.DeleteDMAccount(ctx, phone)
			if err == nil {
				alert = "ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤"
				text, keyboard = s.dmAccountsText(), s.dmAccountsKeyboard()
			}
		case strings.HasPrefix(callback.Data, callbackBlockUser):
			alert, err = s.blockUserCallback(callback.Data)
		case strings.HasPrefix(callback.Data, callbackBlockChat):
			alert, err = s.blockChatCallback(callback.Data)
		default:
			return true, s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ¦Ã¢â‚¬Å“Ã‚ÂÃƒÂ¤Ã‚Â½Ã…â€œ")
		}
	}

	if err == nil && text != "" {
		err = s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, keyboard)
	}

	answerText := alert
	if err != nil {
		answerText = "ÃƒÂ¦Ã¢â‚¬Å“Ã‚ÂÃƒÂ¤Ã‚Â½Ã…â€œÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: " + err.Error()
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
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Â¹ÃƒÂ¦Ã…â€œÃ‚ÂºÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¨Ã†â€™Ã‚Â½ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ§Ã‚Â©Ã‚ÂºÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.backToAccountsKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¦Ã‚Â¬Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â¦Ã‹â€ ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.backToAccountsKeyboard())
		}
		if s.monitorManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã‚Â®Ã‚Â¡ÃƒÂ§Ã‚ÂÃ¢â‚¬Â ÃƒÂ¥Ã¢â€žÂ¢Ã‚Â¨ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.backToAccountsKeyboard())
		}
		result, err := s.monitorManager.StartMonitorLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¥Ã…Â Ã‚Â¨ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: "+err.Error(), s.backToAccountsKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.accountsOverviewText(), s.accountsMenuKeyboard())
	case pendingLoginDM:
		phones := splitInputParts(text)
		if len(phones) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Â¹ÃƒÂ¦Ã…â€œÃ‚ÂºÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¨Ã†â€™Ã‚Â½ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ§Ã‚Â©Ã‚ÂºÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
		}
		if len(phones) > 1 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¦Ã‚Â¬Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â¦Ã‹â€ ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
		}
		if s.dmManager == nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã‚Â®Ã‚Â¡ÃƒÂ§Ã‚ÂÃ¢â‚¬Â ÃƒÂ¥Ã¢â€žÂ¢Ã‚Â¨ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
		}
		result, err := s.dmManager.StartDMLogin(ctx, phones[0])
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¥Ã…Â Ã‚Â¨ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: "+err.Error(), s.dmPoolKeyboard())
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, result+"\n\n"+s.dmPoolText(), s.dmPoolKeyboard())
	case pendingAddKeywordsExact:
		added, err := s.keywordStore.AddWithMode("exact", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â  %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingAddKeywordsFuzzy, pendingAddKeywords:
		added, err := s.keywordStore.AddWithMode("fuzzy", splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â  %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", added, s.keywordsText()), s.keywordsKeyboard())
	case pendingRemoveKeyword:
		removed, err := s.keywordStore.Remove(splitInputParts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", removed, s.keywordsText()), s.keywordsKeyboard())
	case pendingSetCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¦Ã‹Å“Ã‚Â¯ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetChatCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¦Ã‹Å“Ã‚Â¯ÃƒÂ©Ã‚ÂÃ…Â¾ÃƒÂ¨Ã‚Â´Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetChatCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTextCooldown:
		minutes, err := parseNonNegativeInt(text)
		if err != nil || minutes < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¦Ã‹Å“Ã‚Â¯ÃƒÂ©Ã‚ÂÃ…Â¾ÃƒÂ¨Ã‚Â´Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetTextCooldownMinutes(minutes); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetTemplate:
		if strings.TrimSpace(text) == "" {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¨Ã†â€™Ã‚Â½ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ§Ã‚Â©Ã‚ÂºÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetDMTemplate(text); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddDMTemplate:
		templates, parseErr := parseDMTemplateInputs(text)
		if parseErr != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, parseErr.Error()+"\n\n"+s.dmTemplateAddPrompt(), s.dmTemplatesKeyboard())
		}
		if len(templates) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Â¨Â¡Ã¦ÂÂ¿Ã¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©ÂºÃ£â‚¬â€š", s.dmTemplatesKeyboard())
		}
		added, err := s.settings.AddDMTemplates(templates)
		if err != nil {
			return err
		}
		if added > 0 {
			_ = s.settings.SetDMTemplate(templates[0])
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã¦Â·Â»Ã¥Å Â  %d Ã¦ÂÂ¡Ã¨Â¯ÂÃ¦Å“Â¯Ã£â‚¬â€š\n\n%s", added, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingAddDMText:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeTextDMTemplate(text), "Ã¦â€“â€¡Ã¦Å“Â¬Ã§â€ºÂ´Ã¥Ââ€˜")
	case pendingAddDMPostBot:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodePostBotDMTemplate(text), "Ã¥â€ â€¦Ã¨Ââ€Bot @PostBot")
	case pendingAddDMForward:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeForwardDMTemplate(text), "Ã©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã¨Â½Â¬Ã¥Ââ€˜")
	case pendingAddDMHidden:
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeHiddenForwardDMTemplate(text), "Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜Ã¦ÂÂ¥Ã¦ÂºÂ")
	case pendingAddDMQuickReply:
		shortcutID, err := parseNonNegativeInt(text)
		if err != nil || shortcutID <= 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â ID Ã¥Â¿â€¦Ã©Â¡Â»Ã¦ËœÂ¯Ã¥Â¤Â§Ã¤ÂºÅ½ 0 Ã§Å¡â€žÃ¦â€¢Â°Ã¥Â­â€”Ã£â‚¬â€š", s.dmTemplateModeKeyboard())
		}
		return s.handleAddSingleDMTemplate(ctx, msg.Chat.ID, model.EncodeQuickReplyDMTemplate(shortcutID), "Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â")
	case pendingRemoveDMTpl:
		removedTargets := resolveDMTemplateRemovals(text, s.settings.ListDMTemplates())
		if len(removedTargets) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "Ã¦Â²Â¡Ã¦Å“â€°Ã¨Â¯â€ Ã¥Ë†Â«Ã¥Ë†Â°Ã¥ÂÂ¯Ã¥Ë†Â Ã©â„¢Â¤Ã§Å¡â€žÃ¨Â¯ÂÃ¦Å“Â¯Ã§Â¼â€“Ã¥ÂÂ·Ã¦Ë†â€“Ã¥â€ â€¦Ã¥Â®Â¹Ã£â‚¬â€š", s.dmTemplatesKeyboard())
		}
		removed, err := s.settings.RemoveDMTemplates(removedTargets)
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("Ã¥Â·Â²Ã¥Ë†Â Ã©â„¢Â¤ %d Ã¦ÂÂ¡Ã¨Â¯ÂÃ¦Å“Â¯Ã£â‚¬â€š\n\n%s", removed, s.dmTemplatesText()), s.dmTemplatesKeyboard())
	case pendingSetMaxLength:
		maxLength, err := parseNonNegativeInt(text)
		if err != nil || maxLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¦Ã‹Å“Ã‚Â¯ÃƒÂ©Ã‚ÂÃ…Â¾ÃƒÂ¨Ã‚Â´Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetMaxMessageLength(maxLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinLength:
		minLength, err := parseNonNegativeInt(text)
		if err != nil || minLength < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¦Ã‹Å“Ã‚Â¯ÃƒÂ©Ã‚ÂÃ…Â¾ÃƒÂ¨Ã‚Â´Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetMinMessageLength(minLength); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingSetMinAge:
		days, err := parseNonNegativeInt(text)
		if err != nil || days < 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾ÃƒÂ¥Ã‚Â¿Ã¢â‚¬Â¦ÃƒÂ©Ã‚Â¡Ã‚Â»ÃƒÂ¦Ã‹Å“Ã‚Â¯ÃƒÂ©Ã‚ÂÃ…Â¾ÃƒÂ¨Ã‚Â´Ã…Â¸ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.rulesKeyboard())
		}
		if err := s.settings.SetMinAccountAgeDays(days); err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n"+s.rulesText(), s.rulesKeyboard())
	case pendingAddChatIDs:
		added, err := s.settings.AddMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â  %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", added, s.chatsText()), s.chatsKeyboard())
	case pendingRemoveChatIDs:
		removed, err := s.settings.RemoveMonitorChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", removed, s.chatsText()), s.chatsKeyboard())
	case pendingUnblockUsers:
		removed, err := s.removeBlockedUsers(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingUnblockChats:
		removed, err := s.removeBlockedChats(parseInt64Parts(text))
		if err != nil {
			return err
		}
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf("ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\n%s", removed, s.blacklistText()), s.blacklistKeyboard())
	case pendingExportTime:
		start, end, err := parseTimeRange(text)
		if err != nil {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´ÃƒÂ¦Ã‚Â Ã‚Â¼ÃƒÂ¥Ã‚Â¼Ã‚ÂÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¹ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\nÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¤Ã‚Â¾Ã¢â‚¬Â¹: 09-28-12:00 | 09-28-18:30", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByTime,
			start:      start,
			end:        end,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, fmt.Sprintf(
			"ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´ÃƒÂ¦Ã‚Â®Ã‚ÂµÃƒÂ¯Ã‚Â¼Ã…Â¡\n%s ~ %s\n\nÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã‚Â Ã‚Â¼ÃƒÂ¥Ã‚Â¼Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡",
			start.Format("01-02 15:04"),
			end.Format("01-02 15:04"),
		), s.exportFormatKeyboard())
	case pendingExportKeyword:
		keywords := splitInputParts(text)
		if len(keywords) == 0 {
			return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¨Ã†â€™Ã‚Â½ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ§Ã‚Â©Ã‚ÂºÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.cancelExportKeyboard())
		}
		s.setExportContext(msg.From.ID, exportContext{
			filterType: exportByWords,
			keywords:   keywords,
		})
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…Â¡"+strings.Join(keywords, ", ")+"\n\nÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã‚Â Ã‚Â¼ÃƒÂ¥Ã‚Â¼Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.exportFormatKeyboard())
	default:
		return nil
	}
}

func (s *AdminService) handlePendingDMUpload(ctx context.Context, msg *model.Message) error {
	if msg.Document == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â `.session` ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œ `.zip` ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
	}
	if s.dmManager == nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã‚Â®Ã‚Â¡ÃƒÂ§Ã‚ÂÃ¢â‚¬Â ÃƒÂ¥Ã¢â€žÂ¢Ã‚Â¨ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
	}

	filename := strings.TrimSpace(msg.Document.FileName)
	lowerName := strings.ToLower(filename)
	if !strings.HasSuffix(lowerName, ".session") && !strings.HasSuffix(lowerName, ".zip") {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ¦Ã‚Â Ã‚Â¼ÃƒÂ¥Ã‚Â¼Ã‚ÂÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¦Ã¢â‚¬ÂÃ‚Â¯ÃƒÂ¦Ã…â€™Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¤Ã‚Â»Ã¢â‚¬Â¦ÃƒÂ¦Ã¢â‚¬ÂÃ‚Â¯ÃƒÂ¦Ã…â€™Ã‚Â `.session` ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œ `.zip`ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
	}

	downloader, ok := s.client.(fileDownloader)
	if !ok {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚Â Bot API ÃƒÂ¥Ã‚Â®Ã‚Â¢ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ§Ã‚Â«Ã‚Â¯ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¦Ã¢â‚¬ÂÃ‚Â¯ÃƒÂ¦Ã…â€™Ã‚ÂÃƒÂ¤Ã‚Â¸Ã¢â‚¬Â¹ÃƒÂ¨Ã‚Â½Ã‚Â½ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmPoolKeyboard())
	}

	if err := s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚Â§Ã¢â‚¬Â¹ÃƒÂ¤Ã‚Â¸Ã¢â‚¬Â¹ÃƒÂ¨Ã‚Â½Ã‚Â½ÃƒÂ¥Ã‚Â¹Ã‚Â¶ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¥ SessionÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ¤Ã‚Â¼Ã…Â¡ÃƒÂ§Ã‚Â¨Ã‚ÂÃƒÂ§Ã‚Â­Ã¢â‚¬Â°ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¤Ã‚Â¼Ã…Â¡ÃƒÂ¥Ã¢â‚¬Å¾Ã‚Â¿ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", nil); err != nil {
		return err
	}

	downloadedName, data, err := downloader.DownloadFile(ctx, msg.Document.FileID)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¤Ã‚Â¸Ã¢â‚¬Â¹ÃƒÂ¨Ã‚Â½Ã‚Â½ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: "+err.Error(), s.dmPoolKeyboard())
	}
	if strings.TrimSpace(filename) == "" {
		filename = downloadedName
	}

	result, err := s.dmManager.ImportDMSessions(ctx, filename, data)
	if err != nil {
		return s.client.SendMessage(ctx, msg.Chat.ID, "ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¥ Session ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: "+err.Error(), s.dmPoolKeyboard())
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
		"JTBot å…³é”®è¯ç›‘æŽ§æœºå™¨äºº\nç‰ˆæœ¬: %s\n\nðŸ“± ç›‘æŽ§è´¦å·: %dåœ¨çº¿ / %dç¦»çº¿\nðŸ”‘ å…³é”®è¯: %dä¸ª\nðŸ’¬ ç§ä¿¡è®°å½•: å‘é€ %d | æˆåŠŸ %d | å¤±è´¥ %d",
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
	return "ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â Telethon `.session` ÃƒÂ¦Ã¢â‚¬â€œÃ¢â‚¬Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¥Ã…â€™Ã¢â‚¬Â¦ÃƒÂ¥Ã‚ÂÃ‚Â«ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚Âª `.session` ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ `.zip` ÃƒÂ¥Ã…Â½Ã¢â‚¬Â¹ÃƒÂ§Ã‚Â¼Ã‚Â©ÃƒÂ¥Ã…â€™Ã¢â‚¬Â¦ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡\n\nÃƒÂ¤Ã‚Â¸Ã…Â ÃƒÂ¤Ã‚Â¼Ã‚Â ÃƒÂ¥Ã‚ÂÃ…Â½ÃƒÂ¤Ã‚Â¼Ã…Â¡ÃƒÂ¨Ã¢â‚¬Â¡Ã‚ÂªÃƒÂ¥Ã…Â Ã‚Â¨ÃƒÂ¦Ã¢â‚¬Â°Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¥ÃƒÂ¥Ã‚Â¹Ã‚Â¶ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â¯Ã¢â‚¬Â¢ÃƒÂ¦Ã¢â‚¬Â¹Ã¢â‚¬Â°ÃƒÂ¨Ã‚ÂµÃ‚Â·ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡"
}

func (s *AdminService) dmAccountsText() string {
	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return "ÃƒÂ¢Ã‚ÂÃ…â€™ ÃƒÂ¦Ã…Â¡Ã¢â‚¬Å¡ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·\n\nÃƒÂ§Ã¢â‚¬Å¡Ã‚Â¹ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Â»ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œÃƒÂ¨Ã‚Â¿Ã…Â¾ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Â¹ÃƒÂ¥Ã…Â Ã‚Â¨ÃƒÂ§Ã¢â€žÂ¢Ã‚Â»ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ§Ã¢â‚¬Å¡Ã‚Â¹ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œÃƒÂ¤Ã‚Â¸Ã…Â ÃƒÂ¤Ã‚Â¼Ã‚Â  SessionÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã¢â‚¬Â°Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¥ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡"
	}

	lines := []string{fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‹â€ Ã¢â‚¬â€ÃƒÂ¨Ã‚Â¡Ã‚Â¨ (%dÃƒÂ¤Ã‚Â¸Ã‚Âª)ÃƒÂ¯Ã‚Â¼Ã…Â¡", len(accounts)), ""}
	for i, account := range accounts {
		status := "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â´ ÃƒÂ§Ã‚Â¦Ã‚Â»ÃƒÂ§Ã‚ÂºÃ‚Â¿"
		if account.Online {
			status = "ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢ ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ§Ã‚ÂºÃ‚Â¿"
		}
		line := fmt.Sprintf("%d. %s %s | ÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ %d ÃƒÂ¦Ã‚ÂÃ‚Â¡", i+1, account.Phone, status, account.TodaySent)
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
		return "", nil, fmt.Errorf("ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ¥Ã‚Â­Ã‹Å“ÃƒÂ¥Ã…â€œÃ‚Â¨")
	}

	status := "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â´ ÃƒÂ§Ã‚Â¦Ã‚Â»ÃƒÂ§Ã‚ÂºÃ‚Â¿"
	if account.Online {
		status = "ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢ ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ§Ã‚ÂºÃ‚Â¿"
	}
	text := fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¬ ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¨Ã‚Â¯Ã‚Â¦ÃƒÂ¦Ã†â€™Ã¢â‚¬Â¦\n\nÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Â¹ÃƒÂ¦Ã…â€œÃ‚ÂºÃƒÂ¥Ã‚ÂÃ‚Â·: %s\nÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â: %s\nSession: %s\nÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â: %d ÃƒÂ¦Ã‚ÂÃ‚Â¡\nÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¦Ã‹â€ Ã‚ÂÃƒÂ¥Ã…Â Ã…Â¸: %d ÃƒÂ¦Ã‚ÂÃ‚Â¡\nÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: %d ÃƒÂ¦Ã‚ÂÃ‚Â¡", account.Phone, status, filepathBase(account.SessionFile), account.TodaySent, account.TodaySuccess, account.TodayFailed)
	if strings.TrimSpace(account.LastError) != "" {
		text += "\nÃƒÂ©Ã¢â‚¬ÂÃ¢â€žÂ¢ÃƒÂ¨Ã‚Â¯Ã‚Â¯: " + account.LastError
	}
	if label := dmStatusLabel(account.StatusCode); label != "" {
		text += "\nÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶: " + label
	}
	if strings.TrimSpace(account.StatusSummary) != "" {
		text += "\nSpamBot ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã‚ÂµÃ¢â‚¬Â¹: " + account.StatusSummary
		if !account.StatusCheckedAt.IsZero() {
			text += "\nÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã‚ÂµÃ¢â‚¬Â¹ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´: " + account.StatusCheckedAt.Format("2006-01-02 15:04:05")
		}
	}

	keyboard := &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…Â½ ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â", CallbackData: callbackDMCheckPrefix + account.Phone},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Å¾ ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ¨Ã‚Â¿Ã…Â¾ÃƒÂ¦Ã…Â½Ã‚Â¥", CallbackData: callbackDMRetryPrefix + account.Phone},
			},
			{
				{Text: "ÃƒÂ¢Ã‚ÂÃ…â€™ ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackDMDeletePrefix + account.Phone},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¥Ã‹â€ Ã¢â‚¬â€ÃƒÂ¨Ã‚Â¡Ã‚Â¨", CallbackData: callbackDMList},
			},
		},
	}
	return text, keyboard, nil
}

func (s *AdminService) dmTemplatesText() string {
	templates := s.settings.ListDMTemplates()
	if len(templates) == 0 {
		return strings.Join([]string{
			"Ã°Å¸â€œÂ Ã¦Å¡â€šÃ¦â€”Â Ã§Â§ÂÃ¤Â¿Â¡Ã¨Â¯ÂÃ¦Å“Â¯Ã¦Â¨Â¡Ã¦ÂÂ¿",
			"",
			"Ã¦â€“Â°Ã¥Â¢Å¾Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼ÂÃ¯Â¼Å¡",
			"Ã°Å¸â€Â´ Ã¦â€“â€¡Ã¦Å“Â¬Ã§â€ºÂ´Ã¥Ââ€˜: 0 Ã¦ÂÂ¡",
			"Ã°Å¸â€Â´ Ã¥â€ â€¦Ã¨Ââ€Bot @PostBot: 0 Ã¦ÂÂ¡",
			"Ã°Å¸â€Â´ Ã©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã¨Â½Â¬Ã¥Ââ€˜: 0 Ã¦ÂÂ¡",
			"Ã°Å¸â€Â´ Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜Ã¦ÂÂ¥Ã¦ÂºÂ: 0 Ã¦ÂÂ¡",
			"Ã°Å¸â€Â´ Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â: 0 Ã¦ÂÂ¡",
		}, "\n")
	}

	counts := countDMTemplateModes(templates)
	lines := []string{
		fmt.Sprintf("Ã°Å¸â€œÂ Ã¨Â¯ÂÃ¦Å“Â¯Ã¦Â¨Â¡Ã¦ÂÂ¿ (%dÃ¦ÂÂ¡)Ã¯Â¼Å¡", len(templates)),
		"",
		fmt.Sprintf("Ã°Å¸â€Â´ Ã¦â€“â€¡Ã¦Å“Â¬Ã§â€ºÂ´Ã¥Ââ€˜: %d Ã¦ÂÂ¡", counts[model.DMTemplateModeText]),
		fmt.Sprintf("Ã°Å¸â€Â´ Ã¥â€ â€¦Ã¨Ââ€Bot @PostBot: %d Ã¦ÂÂ¡", counts[model.DMTemplateModePostBot]),
		fmt.Sprintf("Ã°Å¸â€Â´ Ã©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã¨Â½Â¬Ã¥Ââ€˜: %d Ã¦ÂÂ¡", counts[model.DMTemplateModeForward]),
		fmt.Sprintf("Ã°Å¸â€Â´ Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜Ã¦ÂÂ¥Ã¦ÂºÂ: %d Ã¦ÂÂ¡", counts[model.DMTemplateModeForwardHidden]),
		fmt.Sprintf("Ã°Å¸â€Â´ Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â: %d Ã¦ÂÂ¡", counts[model.DMTemplateModeQuickReply]),
		"",
	}
	for i, item := range templates {
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, model.ParseDMTemplate(item).Summary()))
	}
	return strings.Join(lines, "\n")
}

func (s *AdminService) dmTemplateAddPrompt() string {
	return strings.Join([]string{
		"Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¦Â·Â»Ã¥Å Â Ã§Å¡â€žÃ¨Â¯ÂÃ¦Å“Â¯Ã¯Â¼Å’Ã¥Â¤Å¡Ã¦ÂÂ¡Ã¥ÂÂ¯Ã§â€Â¨ --- Ã¥Ë†â€ Ã©Å¡â€Ã£â‚¬â€š",
		"",
		"Ã¦â€Â¯Ã¦Å’ÂÃ¨Â¿â„¢Ã¥â€¡Â Ã§Â§ÂÃ¦Â Â¼Ã¥Â¼ÂÃ¯Â¼Å¡",
		"1. Ã¦â€“â€¡Ã¦Å“Â¬Ã§â€ºÂ´Ã¥Ââ€˜Ã¯Â¼Å¡Ã§â€ºÂ´Ã¦Å½Â¥Ã¨Â¾â€œÃ¥â€¦Â¥Ã¥â€ â€¦Ã¥Â®Â¹Ã¯Â¼Å’Ã¦Ë†â€“Ã§â€Â¨ Ã¦â€“â€¡Ã¦Å“Â¬::Ã¥â€ â€¦Ã¥Â®Â¹",
		"2. Ã¥â€ â€¦Ã¨Ââ€BotÃ¯Â¼Å¡PostBot::Ã¤Â»Â£Ã§Â Â",
		"3. Ã©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã¨Â½Â¬Ã¥Ââ€˜Ã¯Â¼Å¡Ã¨Â½Â¬Ã¥Ââ€˜::https://t.me/Ã©Â¢â€˜Ã©Ââ€œ/123",
		"4. Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜Ã¦ÂÂ¥Ã¦ÂºÂÃ¯Â¼Å¡Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜::https://t.me/Ã©Â¢â€˜Ã©Ââ€œ/123",
		"5. Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤ÂÃ¯Â¼Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â::123",
	}, "\n")
}

func (s *AdminService) dmTemplateTextPrompt() string {
	return "Ã¥Ââ€˜Ã©â‚¬ÂÃ¦â€“â€¡Ã¦Å“Â¬Ã§Â§ÂÃ¤Â¿Â¡Ã¥â€ â€¦Ã¥Â®Â¹Ã£â‚¬â€š\n\nÃ¦â€Â¯Ã¦Å’ÂÃ¥ÂËœÃ©â€¡ÂÃ¯Â¼Å¡{username} {chat_title} {keywords} {message}"
}

func (s *AdminService) dmTemplatePostBotPrompt() string {
	return "Ã¥Ââ€˜Ã©â‚¬Â PostBot Ã§Å¡â€žÃ¥â€ â€¦Ã¨Ââ€Ã¤Â»Â£Ã§Â ÂÃ£â‚¬â€š\n\nÃ¤Â¾â€¹Ã¥Â¦â€šÃ¯Â¼Å¡abc123"
}

func (s *AdminService) dmTemplateForwardPrompt(hidden bool) string {
	if hidden {
		return "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ©Å¡ÂÃ¨â€”ÂÃ¦ÂÂ¥Ã¦ÂºÂÃ¨Â½Â¬Ã¥Ââ€˜Ã§Å¡â€žÃ©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã©â€œÂ¾Ã¦Å½Â¥Ã£â‚¬â€š\n\nÃ¤Â¾â€¹Ã¥Â¦â€šÃ¯Â¼Å¡https://t.me/channelname/123"
	}
	return "Ã¥Ââ€˜Ã©â‚¬ÂÃ¨Â¦ÂÃ¨Â½Â¬Ã¥Ââ€˜Ã§Å¡â€žÃ©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã©â€œÂ¾Ã¦Å½Â¥Ã£â‚¬â€š\n\nÃ¤Â¾â€¹Ã¥Â¦â€šÃ¯Â¼Å¡https://t.me/channelname/123"
}

func (s *AdminService) dmTemplateQuickReplyPrompt() string {
	return "Ã¥Ââ€˜Ã©â‚¬ÂÃ¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â IDÃ£â‚¬â€š\n\nÃ¤Â¾â€¹Ã¥Â¦â€šÃ¯Â¼Å¡123"
}

func (s *AdminService) handleAddSingleDMTemplate(ctx context.Context, chatID int64, encoded, label string) error {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return s.client.SendMessage(ctx, chatID, "Ã¥â€ â€¦Ã¥Â®Â¹Ã¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©ÂºÃ£â‚¬â€š", s.dmTemplateModeKeyboard())
	}
	added, err := s.settings.AddDMTemplates([]string{encoded})
	if err != nil {
		return err
	}
	if added > 0 {
		_ = s.settings.SetDMTemplate(encoded)
	}
	return s.client.SendMessage(ctx, chatID, fmt.Sprintf("Ã¥Â·Â²Ã¦Â·Â»Ã¥Å Â  %s Ã¨Â¯ÂÃ¦Å“Â¯Ã£â‚¬â€š\n\n%s", label, s.dmTemplatesText()), s.dmTemplatesKeyboard())
}

func (s *AdminService) dmRecordsText() string {
	records := s.recordStore.DMRecords()
	if len(records) == 0 {
		return "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¨ ÃƒÂ¦Ã…Â¡Ã¢â‚¬Å¡ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢"
	}

	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	lines := []string{
		fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¨ ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢\n\nÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â: %d | ÃƒÂ¦Ã‹â€ Ã‚ÂÃƒÂ¥Ã…Â Ã…Â¸ %d | ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥ %d", todaySent, todaySuccess, todayFailed),
		"",
		"ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Ëœ 10 ÃƒÂ¦Ã‚ÂÃ‚Â¡ÃƒÂ¯Ã‚Â¼Ã…Â¡",
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
		line := fmt.Sprintf("ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ %s | %s | via %s", target, record.Status, senderLabel)
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
		"ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®\n\nÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´: %d ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸\nÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¥Ã‚Â¼Ã‚Â: %s\nÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ©Ã‚Â»Ã‹Å“ÃƒÂ¨Ã‚Â®Ã‚Â¤ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿:\n%s",
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		state.DMTemplate,
	)
}

func (s *AdminService) exportText() string {
	total := len(s.recordStore.MatchRecords())
	return fmt.Sprintf(
		"ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¦Ã‚ÂÃ‚Â®ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº\n\nÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¥Ã¢â‚¬ËœÃ‚Â½ÃƒÂ¤Ã‚Â¸Ã‚Â­ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢: %d ÃƒÂ¦Ã‚ÂÃ‚Â¡\n\nÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¯Ã‚Â¼Ã…Â¡\nÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ÃƒÂ¦Ã…â€™Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´ÃƒÂ¦Ã‚Â®Ã‚ÂµÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº\nÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ÃƒÂ¦Ã…â€™Ã¢â‚¬Â°ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº\nÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¨ÃƒÂ©Ã†â€™Ã‚Â¨ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¦Ã‚ÂÃ‚Â®",
		total,
	)
}

func (s *AdminService) exportTimePrompt() string {
	return "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¦ ÃƒÂ¦Ã…â€™Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´ÃƒÂ¦Ã‚Â®Ã‚ÂµÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº\n\nÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ¨Ã‚Â¾Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¥ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´ÃƒÂ¨Ã…â€™Ã†â€™ÃƒÂ¥Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¯Ã‚Â¼Ã…Â¡\nÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¤Ã‚Â¾Ã¢â‚¬Â¹ 1: 09-28-12:00 | 09-28-18:30\nÃƒÂ§Ã‚Â¤Ã‚ÂºÃƒÂ¤Ã‚Â¾Ã¢â‚¬Â¹ 2: 2026-09-28 12:00 | 2026-09-28 18:30"
}

func (s *AdminService) exportKeywordPrompt() string {
	keywords := s.keywordStore.List()
	return "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Ëœ ÃƒÂ¦Ã…â€™Ã¢â‚¬Â°ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº\n\nÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…Â¡\n" + strings.Join(keywords, " | ") + "\n\nÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ¨Ã‚Â¾Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¥ÃƒÂ¨Ã‚Â¦Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â¤Ã…Â¡ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ | ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã…Â¡Ã¢â‚¬ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡"
}

func (s *AdminService) legacyRulesText() string {
	state := s.settings.Snapshot()
	alertChat := "ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®\n\nÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³: %s\nÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´: %d ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸\nÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¥Ã‚Â¼Ã‚Â: %s\nÃƒÂ©Ã¢â€šÂ¬Ã…Â¡ÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â¾Ã‚Â¤: %s\nÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦: %s\nÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â: %s\nÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ¥Ã‚Â¤Ã‚Â´ÃƒÂ¥Ã†â€™Ã‚Â: %s\nÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾: %s ÃƒÂ¥Ã‚Â¤Ã‚Â©\nÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MaxMessageLength, "ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚Â"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚Â"),
		state.DMTemplate,
	)
}

func (s *AdminService) rulesText() string {
	state := s.settings.Snapshot()
	alertChat := "ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®"
	if state.AlertChatID != 0 {
		alertChat = strconv.FormatInt(state.AlertChatID, 10)
	}
	return fmt.Sprintf(
		"ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®\n\nÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³: %s\nÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´: %d ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸\nÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´: %s ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸\nÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¥Ã‚Â¤Ã‚ÂÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´: %s ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â ÃƒÂ©Ã¢â‚¬â„¢Ã…Â¸\nÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¥Ã‚Â¼Ã‚Â: %s\nÃƒÂ©Ã¢â€šÂ¬Ã…Â¡ÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â¾Ã‚Â¤: %s\nÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦: %s\nÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦: %s\nÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â: %s\nÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ¥Ã‚Â¤Ã‚Â´ÃƒÂ¥Ã†â€™Ã‚Â: %s\nÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾: %s ÃƒÂ¥Ã‚Â¤Ã‚Â©\nÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿:\n%s",
		onOff(state.MonitoringEnabled),
		state.CooldownMinutes,
		formatOptionalNumber(state.ChatCooldownMinutes, "ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­"),
		formatOptionalNumber(state.TextCooldownMinutes, "ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­"),
		dryRunLabel(state.DryRun),
		alertChat,
		formatOptionalNumber(state.MinMessageLength, "ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"),
		formatOptionalNumber(state.MaxMessageLength, "ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"),
		onOff(state.FilterNoUsername),
		onOff(state.FilterNoAvatar),
		formatOptionalNumber(state.MinAccountAgeDays, "ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"),
		state.DMTemplate,
	)
}

func (s *AdminService) chatsText() string {
	state := s.settings.Snapshot()
	body := "ÃƒÂ¦Ã…Â¡Ã¢â‚¬Å¡ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤"
	if len(state.MonitorChatIDs) > 0 {
		parts := make([]string, 0, len(state.MonitorChatIDs))
		for _, id := range state.MonitorChatIDs {
			parts = append(parts, strconv.FormatInt(id, 10))
		}
		body = strings.Join(parts, "\n")
	}
	return fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ¢â‚¬Å¡ ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¦Ã‚ÂÃƒÂ§Ã‚Â½Ã‚Â®\n\nÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â± %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¯Ã‚Â¼Ã…Â¡\n%s", len(state.MonitorChatIDs), body)
}

func (s *AdminService) blacklistText() string {
	if s.blacklist == nil {
		return "ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨"
	}

	users := s.blacklist.Users()
	userBody := "ÃƒÂ¦Ã…Â¡Ã¢â‚¬Å¡ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·"
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
	chatBody := "ÃƒÂ¦Ã…Â¡Ã¢â‚¬Å¡ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã‚Â¾Ã‚Â¤"
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

	return fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â« ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢\n\nÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â· %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¯Ã‚Â¼Ã…Â¡\n%s\n\nÃƒÂ§Ã‚Â¾Ã‚Â¤ %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¯Ã‚Â¼Ã…Â¡\n%s", len(users), userBody, len(chats), chatBody)
}

func (s *AdminService) statusText() string {
	active, total := 0, 0
	if s.monitorManager != nil {
		active, total = s.monitorManager.MonitorCounts()
	}
	matchRecords := s.recordStore.MatchRecords()
	todaySent, todaySuccess, todayFailed := s.dmStatsToday()
	return fmt.Sprintf(
		"ðŸ“Š è¿è¡ŒçŠ¶æ€\n\nç‰ˆæœ¬: %s\nç›‘æŽ§è´¦å·: %dåœ¨çº¿ / %dç¦»çº¿\nå…³é”®è¯: %dä¸ª\nå‘½ä¸­è®°å½•: %d\nç§ä¿¡è®°å½•: å‘é€ %d | æˆåŠŸ %d | å¤±è´¥ %d\nè¿‡æ»¤å¼€å…³: %s\nå‘é€æ¨¡å¼: %s",
		version.Current,
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
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â± ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackAccounts},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ§Ã‚Â®Ã‚Â¡ÃƒÂ§Ã‚ÂÃ¢â‚¬Â ", CallbackData: callbackKeywords},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬â„¢Ã‚Â¬ ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¦Ã‚Â±Ã‚Â ", CallbackData: callbackDMPool},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¦Ã‚ÂÃ‚Â®ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº", CallbackData: callbackExport},
			},
			{
				{Text: "ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®", CallbackData: callbackFilters},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  ÃƒÂ¨Ã‚Â¿Ã‚ÂÃƒÂ¨Ã‚Â¡Ã…â€™ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â", CallbackData: callbackStatus},
			},
		},
	}
}

func (s *AdminService) accountsMenuKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ¢Ã…Â¾Ã¢â‚¬Â¢ ÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ¦Ã¢â‚¬â€œÃ‚Â°ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackAccountAdd},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‹â€ Ã¢â‚¬â€ÃƒÂ¨Ã‚Â¡Ã‚Â¨", CallbackData: callbackAccountList},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) accountsListKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listMonitorAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+1)
	for _, account := range accounts {
		status := "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â´"
		if account.Online {
			status = "ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackAccountDetailPrefix + account.Phone},
		})
	}
	rows = append(rows, []model.InlineKeyboardButton{
		{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackAccounts},
	})
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) backToAccountsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackAccounts}},
		},
	}
}

func (s *AdminService) keywordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ¢Ã…Â¾Ã¢â‚¬Â¢ ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ§Ã‚Â³Ã…Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â", CallbackData: callbackKeywordAdd},
				{Text: "ÃƒÂ°Ã…Â¸Ã…Â½Ã‚Â¯ ÃƒÂ§Ã‚Â²Ã‚Â¾ÃƒÂ¥Ã¢â‚¬Â¡Ã¢â‚¬Â ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â", CallbackData: callbackKeywordAdd + ":exact"},
			},
			{
				{Text: "ÃƒÂ¢Ã…Â¾Ã¢â‚¬â€œ ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â", CallbackData: callbackKeywordRemove},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmPoolKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…â€™ ÃƒÂ¨Ã‚Â¿Ã…Â¾ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackDMConnect},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¤Ã‚Â¸Ã…Â ÃƒÂ¤Ã‚Â¼Ã‚Â  Session", CallbackData: callbackDMUpload},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‹â€ Ã¢â‚¬â€ÃƒÂ¨Ã‚Â¡Ã‚Â¨", CallbackData: callbackDMList},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â", CallbackData: callbackDMCheckAll},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¦Ã…â€œÃ‚Â¯ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿", CallbackData: callbackDMTemplates},
				{Text: "ÃƒÂ¢Ã…Â¡Ã¢â€žÂ¢ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®", CallbackData: callbackDMSettings},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¨ ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚ÂÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢", CallbackData: callbackDMRecords},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain},
			},
		},
	}
}

func (s *AdminService) dmAccountsKeyboard() *model.InlineKeyboardMarkup {
	accounts := s.listDMAccounts()
	rows := make([][]model.InlineKeyboardButton, 0, len(accounts)+2)
	for _, account := range accounts {
		status := "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â´"
		if account.Online {
			status = "ÃƒÂ°Ã…Â¸Ã…Â¸Ã‚Â¢"
		}
		rows = append(rows, []model.InlineKeyboardButton{
			{Text: status + " " + account.Phone, CallbackData: callbackDMDetailPrefix + account.Phone},
		})
	}
	rows = append(rows,
		[]model.InlineKeyboardButton{
			{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…â€™ ÃƒÂ¨Ã‚Â¿Ã…Â¾ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackDMConnect},
			{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¤Ã‚Â¸Ã…Â ÃƒÂ¤Ã‚Â¼Ã‚Â  Session", CallbackData: callbackDMUpload},
		},
		[]model.InlineKeyboardButton{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â ÃƒÂ¤Ã‚Â¸Ã¢â€šÂ¬ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â", CallbackData: callbackDMCheckAll}},
		[]model.InlineKeyboardButton{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackDMPool}},
	)
	return &model.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (s *AdminService) dmTemplatesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã¢Å¾â€¢ Ã¦Â·Â»Ã¥Å Â Ã¨Â¯ÂÃ¦Å“Â¯", CallbackData: callbackDMTemplateAdd},
				{Text: "Ã¢Å¾â€“ Ã¥Ë†Â Ã©â„¢Â¤Ã¨Â¯ÂÃ¦Å“Â¯", CallbackData: callbackDMTemplateRemove},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmTemplateModeKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "Ã°Å¸â€œÂ Ã¦â€“â€¡Ã¦Å“Â¬Ã§â€ºÂ´Ã¥Ââ€˜", CallbackData: callbackDMTemplateAddText},
				{Text: "Ã°Å¸Â¤â€“ Ã¥â€ â€¦Ã¨Ââ€Bot", CallbackData: callbackDMTemplateAddPost},
			},
			{
				{Text: "Ã°Å¸â€œÂ¢ Ã©Â¢â€˜Ã©Ââ€œÃ¨Â½Â¬Ã¥Ââ€˜", CallbackData: callbackDMTemplateAddFwd},
				{Text: "Ã°Å¸â€¢Â¶ Ã©Å¡ÂÃ¨â€”ÂÃ¦ÂÂ¥Ã¦ÂºÂÃ¨Â½Â¬Ã¥Ââ€˜", CallbackData: callbackDMTemplateAddHide},
			},
			{
				{Text: "Ã°Å¸ÂÂ¢ Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â", CallbackData: callbackDMTemplateAddQuick},
			},
			{
				{Text: "Ã°Å¸â€â„¢ Ã¨Â¿â€Ã¥â€ºÅ¾Ã¨Â¯ÂÃ¦Å“Â¯Ã¥Ë†â€”Ã¨Â¡Â¨", CallbackData: callbackDMTemplates},
			},
		},
	}
}

func (s *AdminService) dmRecordsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackDMExportFailed},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) dmSettingsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¢ Dry-run", CallbackData: callbackToggleDryRun},
				{Text: "ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´", CallbackData: callbackSetCooldown},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackDMPool},
			},
		},
	}
}

func (s *AdminService) exportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¦ ÃƒÂ¦Ã…â€™Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´ÃƒÂ¦Ã‚Â®Ã‚ÂµÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº", CallbackData: callbackExportByTime}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â‚¬Ëœ ÃƒÂ¦Ã…â€™Ã¢â‚¬Â°ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚ÂÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº", CallbackData: callbackExportByKeyword}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â¨ÃƒÂ©Ã†â€™Ã‚Â¨ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¦Ã‚ÂÃ‚Â®", CallbackData: callbackExportAll}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) exportFormatKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ËœÃ‚Â¤ ÃƒÂ¤Ã‚Â»Ã¢â‚¬Â¦ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â (TXT)", CallbackData: callbackExportFormatUsers}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Â Ã¢â‚¬Â ÃƒÂ¤Ã‚Â»Ã¢â‚¬Â¦ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ID (TXT)", CallbackData: callbackExportFormatIDs}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  ÃƒÂ¥Ã‚Â®Ã…â€™ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â´ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ (CSV)", CallbackData: callbackExportFormatCSV}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) cancelExportKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¥Ã‚ÂÃ¢â‚¬â€œÃƒÂ¦Ã‚Â¶Ã‹â€ ", CallbackData: callbackExport}},
		},
	}
}

func (s *AdminService) legacyRulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§", CallbackData: callbackToggleMonitor}, {Text: "ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¢ Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´", CallbackData: callbackSetCooldown}, {Text: "ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿", CallbackData: callbackSetTemplate}},
			{{Text: "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦", CallbackData: callbackSetMaxLength}, {Text: "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾", CallbackData: callbackSetMinAge}},
			{{Text: "ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â", CallbackData: callbackToggleNoName}, {Text: "ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ¥Ã‚Â¤Ã‚Â´ÃƒÂ¥Ã†â€™Ã‚Â", CallbackData: callbackToggleNoPhoto}},
			{{Text: "ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¦Ã‚ÂÃƒÂ§Ã‚Â½Ã‚Â®", CallbackData: callbackChats}, {Text: "ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackBlacklist}},
			{{Text: "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¨Ã‚ÂÃ…Â ÃƒÂ¥Ã‚Â¤Ã‚Â©ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ©Ã¢â€šÂ¬Ã…Â¡ÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â¾Ã‚Â¤", CallbackData: callbackSetAlertChat}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) rulesKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§", CallbackData: callbackToggleMonitor}, {Text: "ÃƒÂ¥Ã‹â€ Ã¢â‚¬Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¢ Dry-run", CallbackData: callbackToggleDryRun}},
			{{Text: "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´", CallbackData: callbackSetCooldown}, {Text: "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´", CallbackData: callbackSetChatCooldown}},
			{{Text: "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ¥Ã¢â‚¬Â Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â´", CallbackData: callbackSetTextCooldown}, {Text: "ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ§Ã‚Â½Ã‚Â®ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¦Ã‚ÂÃ‚Â¿", CallbackData: callbackSetTemplate}},
			{{Text: "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦", CallbackData: callbackSetMinLength}, {Text: "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â¤Ã‚Â§ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¥Ã‚ÂºÃ‚Â¦", CallbackData: callbackSetMaxLength}},
			{{Text: "ÃƒÂ¦Ã…â€œÃ¢â€šÂ¬ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â¹Ã‚Â´ÃƒÂ©Ã‚Â¾Ã¢â‚¬Å¾", CallbackData: callbackSetMinAge}, {Text: "ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â", CallbackData: callbackToggleNoName}},
			{{Text: "ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ¦Ã‚Â»Ã‚Â¤ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ¥Ã‚Â¤Ã‚Â´ÃƒÂ¥Ã†â€™Ã‚Â", CallbackData: callbackToggleNoPhoto}},
			{{Text: "ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ©Ã¢â‚¬Â¦Ã‚ÂÃƒÂ§Ã‚Â½Ã‚Â®", CallbackData: callbackChats}, {Text: "ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackBlacklist}},
			{{Text: "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¨Ã‚ÂÃ…Â ÃƒÂ¥Ã‚Â¤Ã‚Â©ÃƒÂ¨Ã‚Â®Ã‚Â¾ÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ©Ã¢â€šÂ¬Ã…Â¡ÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â¾Ã‚Â¤", CallbackData: callbackSetAlertChat}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) chatsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ¢Ã…Â¾Ã¢â‚¬Â¢ ÃƒÂ¦Ã‚Â·Ã‚Â»ÃƒÂ¥Ã…Â Ã‚Â ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤", CallbackData: callbackAddChat}, {Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã¢â‚¬Ëœ ÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚Â¬ÃƒÂ§Ã‚Â¾Ã‚Â¤", CallbackData: callbackRemoveChat}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) blacklistKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·", CallbackData: callbackUnblockUser}, {Text: "ÃƒÂ§Ã‚Â§Ã‚Â»ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ§Ã‚Â¾Ã‚Â¤", CallbackData: callbackUnblockChat}},
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾", CallbackData: callbackFilters}},
		},
	}
}

func (s *AdminService) statusKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¤Ã‚Â¸Ã‚Â»ÃƒÂ¨Ã‚ÂÃ…â€œÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", CallbackData: callbackMain}},
		},
	}
}

func (s *AdminService) blockUserCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨", nil
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
		return "ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", nil
	}
	return "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬Â¹Ã¢â‚¬Â°ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·", nil
}

func (s *AdminService) blockChatCallback(data string) (string, error) {
	if s.blacklist == nil {
		return "ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨", nil
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
		return "ÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ¥Ã‚ÂÃ‚ÂÃƒÂ¥Ã‚ÂÃ¢â‚¬Â¢", nil
	}
	return "ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã¢â‚¬Â¹Ã¢â‚¬Â°ÃƒÂ©Ã‚Â»Ã¢â‚¬ËœÃƒÂ§Ã‚Â¾Ã‚Â¤", nil
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
		return fmt.Errorf("ÃƒÂ¨Ã‚Â¯Ã‚Â·ÃƒÂ¥Ã¢â‚¬Â¦Ã‹â€ ÃƒÂ©Ã¢â€šÂ¬Ã¢â‚¬Â°ÃƒÂ¦Ã¢â‚¬Â¹Ã‚Â©ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã‚ÂÃ‚Â¡ÃƒÂ¤Ã‚Â»Ã‚Â¶")
	}

	records := s.filterMatchRecords(exportCtx)
	if len(records) == 0 {
		return fmt.Errorf("ÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã…â€™Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¦Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â°ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ¦Ã¢â‚¬Â¢Ã‚Â°ÃƒÂ¦Ã‚ÂÃ‚Â®")
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
		return fmt.Errorf("ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ§Ã…Â¸Ã‚Â¥ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã‚Â Ã‚Â¼ÃƒÂ¥Ã‚Â¼Ã‚Â")
	}
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return fmt.Errorf("ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ§Ã‚Â»Ã¢â‚¬Å“ÃƒÂ¦Ã…Â¾Ã…â€œÃƒÂ¤Ã‚Â¸Ã‚ÂºÃƒÂ§Ã‚Â©Ã‚Âº")
	}

	caption := fmt.Sprintf("ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã‚Â®Ã…â€™ÃƒÂ¦Ã‹â€ Ã‚ÂÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â± %d ÃƒÂ¦Ã‚ÂÃ‚Â¡ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", len(records))
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
	if err := writer.Write([]string{"ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ID", "ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ¦Ã‹â€ Ã‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â", "ÃƒÂ¦Ã‹Å“Ã‚ÂµÃƒÂ§Ã‚Â§Ã‚Â°", "ÃƒÂ¦Ã‚ÂÃ‚Â¥ÃƒÂ¦Ã‚ÂºÃ‚ÂÃƒÂ§Ã‚Â¾Ã‚Â¤ÃƒÂ§Ã‚Â»Ã¢â‚¬Å¾", "ÃƒÂ¨Ã‚Â§Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬ÂÃ‚Â®ÃƒÂ¨Ã‚Â¯Ã‚Â", "ÃƒÂ¨Ã‚Â§Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´", "ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹", "ÃƒÂ§Ã¢â‚¬ÂºÃ¢â‚¬ËœÃƒÂ¦Ã…Â½Ã‚Â§ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·"}); err != nil {
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
		return "", fmt.Errorf("Ã¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼ÂÃ¥â€ â€¦Ã¥Â®Â¹Ã¤Â¸ÂÃ¨Æ’Â½Ã¤Â¸ÂºÃ§Â©Âº")
	}

	switch mode {
	case "Ã¦â€“â€¡Ã¦Å“Â¬", "Ã¦â€“â€¡Ã¦Å“Â¬Ã§â€ºÂ´Ã¥Ââ€˜", "text":
		return model.EncodeTextDMTemplate(value), nil
	case "postbot", "Ã¥â€ â€¦Ã¨Ââ€bot", "Ã¥â€ â€¦Ã¨Ââ€":
		return model.EncodePostBotDMTemplate(value), nil
	case "Ã¨Â½Â¬Ã¥Ââ€˜", "Ã©Â¢â€˜Ã©Ââ€œÃ¨Â½Â¬Ã¥Ââ€˜", "Ã©Â¢â€˜Ã©Ââ€œÃ¨Â´Â´Ã¦â€“â€¡Ã¨Â½Â¬Ã¥Ââ€˜", "forward":
		return model.EncodeForwardDMTemplate(value), nil
	case "Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜", "Ã©Å¡ÂÃ¨â€”ÂÃ¦ÂÂ¥Ã¦ÂºÂ", "Ã©Å¡ÂÃ¨â€”ÂÃ¨Â½Â¬Ã¥Ââ€˜Ã¦ÂÂ¥Ã¦ÂºÂ", "forward_hidden", "hidden_forward":
		return model.EncodeHiddenForwardDMTemplate(value), nil
	case "Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â", "Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â", "quick_reply", "quickreply":
		shortcutID, err := strconv.Atoi(value)
		if err != nil || shortcutID <= 0 {
			return "", fmt.Errorf("Ã¤Â¼ÂÃ¤Â¸Å¡Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤ÂÃ¦Â Â¼Ã¥Â¼ÂÃ¤Â¸ÂÃ¥Â¯Â¹Ã¯Â¼Å’Ã¨Â¯Â·Ã§â€Â¨ Ã¥Â¿Â«Ã¦ÂÂ·Ã¥â€ºÅ¾Ã¥Â¤Â::123 Ã¨Â¿â„¢Ã§Â§ÂÃ¦Â Â¼Ã¥Â¼Â")
		}
		return model.EncodeQuickReplyDMTemplate(shortcutID), nil
	default:
		return "", fmt.Errorf("Ã¤Â¸ÂÃ¦â€Â¯Ã¦Å’ÂÃ§Å¡â€žÃ¥Ââ€˜Ã©â‚¬ÂÃ¦Â¨Â¡Ã¥Â¼ÂÃ¯Â¼Å¡%s", strings.TrimSpace(parts[0]))
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
		return "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚ÂÃ‚Â¯"
	}
	return "ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â³ÃƒÂ©Ã¢â‚¬â€Ã‚Â­"
}

func formatOptionalNumber(value int, disabled string) string {
	if value <= 0 {
		return disabled
	}
	return strconv.Itoa(value)
}

func dryRunLabel(enabled bool) string {
	if enabled {
		return "ÃƒÂ¦Ã‚Â¼Ã¢â‚¬ÂÃƒÂ§Ã‚Â»Ã†â€™ÃƒÂ¦Ã‚Â¨Ã‚Â¡ÃƒÂ¥Ã‚Â¼Ã‚ÂÃƒÂ¯Ã‚Â¼Ã‹â€ ÃƒÂ¤Ã‚Â¸Ã‚ÂÃƒÂ§Ã…â€œÃ…Â¸ÃƒÂ¥Ã‚Â®Ã…Â¾ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¯Ã‚Â¼Ã¢â‚¬Â°"
	}
	return "ÃƒÂ§Ã…â€œÃ…Â¸ÃƒÂ¥Ã‚Â®Ã…Â¾ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â"
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
	return string(runes[:limit-12]) + "\n\nÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ ÃƒÂ¥Ã¢â‚¬Â Ã¢â‚¬Â¦ÃƒÂ¥Ã‚Â®Ã‚Â¹ÃƒÂ¨Ã‚Â¿Ã¢â‚¬Â¡ÃƒÂ©Ã¢â‚¬Â¢Ã‚Â¿ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã‹â€ Ã‚ÂªÃƒÂ¦Ã¢â‚¬â€œÃ‚Â­"
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
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ§Ã¢â‚¬ÂÃ‚Â¨ÃƒÂ§Ã…Â¡Ã¢â‚¬Å¾ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã‚Â®Ã‚Â¡ÃƒÂ§Ã‚ÂÃ¢â‚¬Â ÃƒÂ¥Ã¢â€žÂ¢Ã‚Â¨")
	}

	accounts := s.listDMAccounts()
	if len(accounts) == 0 {
		return s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥")
	}

	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚Â§Ã¢â‚¬Â¹ÃƒÂ¦Ã¢â‚¬Â°Ã‚Â¹ÃƒÂ©Ã¢â‚¬Â¡Ã‚ÂÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â"); err != nil {
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
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚Â§Ã¢â‚¬Â¹ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã¢â‚¬Â°Ã¢â€šÂ¬ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ©Ã†â€™Ã‚Â½ÃƒÂ¥Ã‚Â±Ã…Â¾ÃƒÂ¤Ã‚ÂºÃ…Â½ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚ÂÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmCheckActionsKeyboard())
	}

	if err := s.sendDMAccountExportDocuments(ctx, callback.Message.Chat.ID, accounts, "ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·"); err != nil {
		return err
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº\n\nÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚Âº: %d ÃƒÂ¤Ã‚Â¸Ã‚Âª\nÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤: %d ÃƒÂ¤Ã‚Â¸Ã‚Âª\nÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: %d ÃƒÂ¤Ã‚Â¸Ã‚Âª\n\nÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¤Ã‚Â¿Ã‚ÂÃƒÂ§Ã¢â‚¬Â¢Ã¢â€žÂ¢: ÃƒÂ¤Ã‚Â»Ã¢â‚¬Â¦ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·", len(accounts), removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) handleDMKeepOnlyNormal(ctx context.Context, callback *model.CallbackQuery) error {
	if err := s.client.AnswerCallbackQuery(ctx, callback.ID, "ÃƒÂ¥Ã‚Â¼Ã¢â€šÂ¬ÃƒÂ¥Ã‚Â§Ã¢â‚¬Â¹ÃƒÂ¦Ã‚Â¸Ã¢â‚¬Â¦ÃƒÂ§Ã‚ÂÃ¢â‚¬Â ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·"); err != nil {
		return err
	}

	accounts := s.collectDMAccountsByStatus(false)
	if len(accounts) == 0 {
		return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, "ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ§Ã‚Â»Ã‚ÂÃƒÂ¥Ã‚ÂÃ‚ÂªÃƒÂ¤Ã‚Â¿Ã‚ÂÃƒÂ§Ã¢â‚¬Â¢Ã¢â€žÂ¢ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", s.dmAccountsKeyboard())
	}

	removed, failed := s.deleteDMAccounts(ctx, accounts)
	text := fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â¹ ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¦Ã‚Â±Ã‚Â ÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¦Ã‚Â¸Ã¢â‚¬Â¦ÃƒÂ§Ã‚ÂÃ¢â‚¬Â \n\nÃƒÂ¥Ã‚Â·Ã‚Â²ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·: %d ÃƒÂ¤Ã‚Â¸Ã‚Âª\nÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: %d ÃƒÂ¤Ã‚Â¸Ã‚Âª\nÃƒÂ¥Ã‚Â½Ã¢â‚¬Å“ÃƒÂ¥Ã¢â‚¬Â°Ã‚ÂÃƒÂ¥Ã‚ÂÃ‚ÂªÃƒÂ¤Ã‚Â¿Ã‚ÂÃƒÂ§Ã¢â‚¬Â¢Ã¢â€žÂ¢ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡", removed, failed)
	return s.editMessageText(ctx, callback.Message.Chat.ID, callback.Message.MessageID, text, s.dmAccountsKeyboard())
}

func (s *AdminService) dmCheckProgressText(done, total int, counts map[string]int) string {
	return fmt.Sprintf(
		"ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã…â€œÃ‚Â¨ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â (%d/%d)\n\n%s",
		done,
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckResultText(total int, counts map[string]int) string {
	return fmt.Sprintf(
		"ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚ÂÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ¥Ã‚Â®Ã…â€™ÃƒÂ¦Ã‹â€ Ã‚Â\n\nÃƒÂ¦Ã¢â€šÂ¬Ã‚Â»ÃƒÂ¨Ã‚Â®Ã‚Â¡: %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·\n%s\nÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ¤Ã‚Â¸Ã¢â‚¬Â¹ÃƒÂ¦Ã‚ÂÃ‚Â¥ÃƒÂ¤Ã‚Â½Ã‚Â ÃƒÂ¥Ã‚ÂÃ‚Â¯ÃƒÂ¤Ã‚Â»Ã‚Â¥ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¯Ã‚Â¼Ã…â€™ÃƒÂ¦Ã‹â€ Ã¢â‚¬â€œÃƒÂ¨Ã¢â€šÂ¬Ã¢â‚¬Â¦ÃƒÂ§Ã¢â‚¬ÂºÃ‚Â´ÃƒÂ¦Ã…Â½Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ‚ÂªÃƒÂ¤Ã‚Â¿Ã‚ÂÃƒÂ§Ã¢â‚¬Â¢Ã¢â€žÂ¢ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ£Ã¢â€šÂ¬Ã¢â‚¬Å¡",
		total,
		formatDMStatusCounts(counts),
	)
}

func (s *AdminService) dmCheckActionsKeyboard() *model.InlineKeyboardMarkup {
	return &model.InlineKeyboardMarkup{
		InlineKeyboard: [][]model.InlineKeyboardButton{
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¤ ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¥Ã‚Â¹Ã‚Â¶ÃƒÂ¥Ã‹â€ Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚Â¤", CallbackData: callbackDMExportAbnormal},
				{Text: "ÃƒÂ°Ã…Â¸Ã‚Â§Ã‚Â¹ ÃƒÂ¤Ã‚Â»Ã¢â‚¬Â¦ÃƒÂ¤Ã‚Â¿Ã‚ÂÃƒÂ§Ã¢â‚¬Â¢Ã¢â€žÂ¢ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·", CallbackData: callbackDMKeepOnlyNormal},
			},
			{
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¥Ã‹â€ Ã¢â‚¬â€ÃƒÂ¨Ã‚Â¡Ã‚Â¨", CallbackData: callbackDMList},
				{Text: "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ¢â€žÂ¢ ÃƒÂ¨Ã‚Â¿Ã¢â‚¬ÂÃƒÂ¥Ã¢â‚¬ÂºÃ…Â¾ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¥Ã‚ÂÃ‚Â·ÃƒÂ¦Ã‚Â±Ã‚Â ", CallbackData: callbackDMPool},
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
		caption := fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¦ %s Session ÃƒÂ¦Ã¢â‚¬Â°Ã¢â‚¬Å“ÃƒÂ¥Ã…â€™Ã¢â‚¬Â¦ÃƒÂ¯Ã‚Â¼Ã‹â€ %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¯Ã‚Â¼Ã¢â‚¬Â°", label, sessionCount)
		if err := s.client.SendDocument(ctx, chatID, filename, zipData, caption); err != nil {
			return err
		}
	}

	reportData := buildDMAccountReport(accounts, label)
	reportName := fmt.Sprintf("dm_abnormal_accounts_%s.txt", timestamp)
	reportCaption := fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ %sÃƒÂ¥Ã‹â€ Ã¢â‚¬â€ÃƒÂ¨Ã‚Â¡Ã‚Â¨ÃƒÂ¯Ã‚Â¼Ã‹â€ %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¯Ã‚Â¼Ã¢â‚¬Â°", label, len(accounts))
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
		fmt.Sprintf("# ÃƒÂ¥Ã‚Â¯Ã‚Â¼ÃƒÂ¥Ã¢â‚¬Â¡Ã‚ÂºÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â‚¬â€Ã‚Â´: %s", time.Now().Format("2006-01-02 15:04:05")),
		fmt.Sprintf("# ÃƒÂ¥Ã¢â‚¬Â¦Ã‚Â± %d ÃƒÂ¤Ã‚Â¸Ã‚ÂªÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·", len(accounts)),
		"",
	}

	for _, account := range accounts {
		code := effectiveDMStatusCode(account)
		line := fmt.Sprintf(
			"%s | %s | ÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€šÂ¬Ã‚Â %d ÃƒÂ¦Ã‚ÂÃ‚Â¡ | ÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¦Ã‹â€ Ã‚ÂÃƒÂ¥Ã…Â Ã…Â¸ %d ÃƒÂ¦Ã‚ÂÃ‚Â¡ | ÃƒÂ¤Ã‚Â»Ã…Â ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¥ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥ %d ÃƒÂ¦Ã‚ÂÃ‚Â¡",
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
		fmt.Sprintf("ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶: %d", counts["active"]),
		fmt.Sprintf("ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¤Ã‚Â¸Ã‚Â´ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ / ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶: %d", counts["restricted"]),
		fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Âµ ÃƒÂ¥Ã…Â¾Ã†â€™ÃƒÂ¥Ã…â€œÃ‚Â¾ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã‚Â£Ã…Â½ÃƒÂ¦Ã…Â½Ã‚Â§: %d", counts["spam"]),
		fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â« ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ§Ã‚Â¦Ã‚ÂÃƒÂ¨Ã‚Â´Ã‚Â¦ÃƒÂ¥Ã‚ÂÃ‚Â·: %d", counts["banned"]),
		fmt.Sprintf("ÃƒÂ¢Ã‚ÂÃ¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¥Ã¢â‚¬Â Ã‚Â»ÃƒÂ§Ã‚Â»Ã¢â‚¬Å“ / ÃƒÂ¥Ã‚Â®Ã‚Â¡ÃƒÂ¦Ã‚Â Ã‚Â¸ÃƒÂ¤Ã‚Â¸Ã‚Â­: %d", counts["frozen"]),
		fmt.Sprintf("ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…â€™ ÃƒÂ§Ã‚Â¦Ã‚Â»ÃƒÂ§Ã‚ÂºÃ‚Â¿ / ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥: %d", counts["failed"]),
	}
	if counts["unknown"] > 0 {
		lines = append(lines, fmt.Sprintf("ÃƒÂ¢Ã‚ÂÃ¢â‚¬Å“ ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¨Ã‚Â¯Ã¢â‚¬Â ÃƒÂ¥Ã‹â€ Ã‚Â«ÃƒÂ§Ã…Â Ã‚Â¶ÃƒÂ¦Ã¢â€šÂ¬Ã‚Â: %d", counts["unknown"]))
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
	case strings.Contains(summary, "ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸"), strings.Contains(summary, "ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"):
		return "active"
	case strings.Contains(summary, "ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã‚ÂÃ¢â‚¬Ëœ"), strings.Contains(summary, "ÃƒÂ¤Ã‚Â¸Ã‚Â´ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"):
		return "restricted"
	case strings.Contains(summary, "ÃƒÂ©Ã‚Â£Ã…Â½ÃƒÂ¦Ã…Â½Ã‚Â§"), strings.Contains(summary, "ÃƒÂ¥Ã…Â¾Ã†â€™ÃƒÂ¥Ã…â€œÃ‚Â¾ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯"):
		return "spam"
	case strings.Contains(summary, "ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ§Ã‚Â¦Ã‚Â"), strings.Contains(summary, "ÃƒÂ¦Ã‚Â°Ã‚Â¸ÃƒÂ¤Ã‚Â¹Ã¢â‚¬Â¦ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"):
		return "banned"
	case strings.Contains(summary, "ÃƒÂ¥Ã‚Â®Ã‚Â¡ÃƒÂ¦Ã‚Â Ã‚Â¸"), strings.Contains(summary, "ÃƒÂ©Ã‚ÂªÃ…â€™ÃƒÂ¨Ã‚Â¯Ã‚Â"), strings.Contains(summary, "ÃƒÂ¥Ã¢â‚¬Â Ã‚Â»ÃƒÂ§Ã‚Â»Ã¢â‚¬Å“"):
		return "frozen"
	case strings.Contains(summary, "ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥"), strings.Contains(summary, "ÃƒÂ§Ã‚Â¦Ã‚Â»ÃƒÂ§Ã‚ÂºÃ‚Â¿"):
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
		return "ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ ÃƒÂ¦Ã‚Â­Ã‚Â£ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ¦Ã¢â‚¬â€Ã‚Â ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"
	case "restricted":
		return "ÃƒÂ¢Ã…Â¡Ã‚Â ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¤Ã‚Â¸Ã‚Â´ÃƒÂ¦Ã¢â‚¬â€Ã‚Â¶ÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶ / ÃƒÂ¥Ã‚ÂÃ…â€™ÃƒÂ¥Ã‚ÂÃ¢â‚¬ËœÃƒÂ©Ã¢â€žÂ¢Ã‚ÂÃƒÂ¥Ã‹â€ Ã‚Â¶"
	case "spam":
		return "ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Âµ ÃƒÂ¥Ã…Â¾Ã†â€™ÃƒÂ¥Ã…â€œÃ‚Â¾ÃƒÂ¦Ã‚Â¶Ã‹â€ ÃƒÂ¦Ã‚ÂÃ‚Â¯ÃƒÂ©Ã‚Â£Ã…Â½ÃƒÂ¦Ã…Â½Ã‚Â§"
	case "banned":
		return "ÃƒÂ°Ã…Â¸Ã…Â¡Ã‚Â« ÃƒÂ¥Ã‚Â°Ã‚ÂÃƒÂ§Ã‚Â¦Ã‚Â"
	case "frozen":
		return "ÃƒÂ¢Ã‚ÂÃ¢â‚¬Å¾ÃƒÂ¯Ã‚Â¸Ã‚Â ÃƒÂ¥Ã¢â‚¬Â Ã‚Â»ÃƒÂ§Ã‚Â»Ã¢â‚¬Å“ / ÃƒÂ¥Ã‚Â®Ã‚Â¡ÃƒÂ¦Ã‚Â Ã‚Â¸ÃƒÂ¤Ã‚Â¸Ã‚Â­"
	case "failed":
		return "ÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ…â€™ ÃƒÂ§Ã‚Â¦Ã‚Â»ÃƒÂ§Ã‚ÂºÃ‚Â¿ / ÃƒÂ¦Ã‚Â£Ã¢â€šÂ¬ÃƒÂ¦Ã…Â¸Ã‚Â¥ÃƒÂ¥Ã‚Â¤Ã‚Â±ÃƒÂ¨Ã‚Â´Ã‚Â¥"
	default:
		return "ÃƒÂ¢Ã‚ÂÃ¢â‚¬Å“ ÃƒÂ¦Ã…â€œÃ‚ÂªÃƒÂ¨Ã‚Â¯Ã¢â‚¬Â ÃƒÂ¥Ã‹â€ Ã‚Â«"
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
		return fmt.Errorf("ÃƒÂ¦Ã‚Â²Ã‚Â¡ÃƒÂ¦Ã…â€œÃ¢â‚¬Â°ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢")
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
	return s.client.SendDocument(ctx, chatID, filename, []byte(strings.Join(lines, "\n")), fmt.Sprintf("ÃƒÂ¥Ã‚Â¼Ã¢â‚¬Å¡ÃƒÂ¥Ã‚Â¸Ã‚Â¸ÃƒÂ§Ã‚Â§Ã‚ÂÃƒÂ¤Ã‚Â¿Ã‚Â¡ÃƒÂ¨Ã‚Â®Ã‚Â°ÃƒÂ¥Ã‚Â½Ã¢â‚¬Â¢ %d ÃƒÂ¦Ã‚ÂÃ‚Â¡", len(failed)))
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

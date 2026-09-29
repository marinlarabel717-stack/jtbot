package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/config"
	"github.com/marinlarabel717-stack/jtbot/internal/listener"
	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/matcher"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/queue"
	"github.com/marinlarabel717-stack/jtbot/internal/rules"
	"github.com/marinlarabel717-stack/jtbot/internal/sender"
	"github.com/marinlarabel717-stack/jtbot/internal/service"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type App struct {
	cfg    config.Config
	logger *logx.Logger

	adminBot    *tg.Client
	adminPoller *listener.Poller

	keywordStore *storage.KeywordStore
	recordStore  *storage.RecordStore
	settings     *storage.SettingsStore
	blacklist    *storage.BlacklistStore
	matcher      *matcher.KeywordMatcher
	ruleEngine   *rules.Engine
	accountStore *storage.MonitorAccountStore

	authCoordinator *tg.AuthCoordinator

	runMu           sync.RWMutex
	runCtx          context.Context
	startedMonitors map[string]struct{}
	wg              sync.WaitGroup
}

func New() (*App, error) {
	_ = config.LoadDotEnv(".env")

	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	logger := logx.New(cfg.LogLevel)

	if err := os.MkdirAll(filepath.Dir(cfg.RecordsFile), 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if err := os.MkdirAll(cfg.SessionsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create sessions dir: %w", err)
	}

	keywordStore := storage.NewKeywordStore(cfg.KeywordsFile)
	keywords, err := keywordStore.Load()
	if err != nil {
		return nil, err
	}
	logger.Infof("loaded %d keywords", len(keywords))

	recordStore, err := storage.NewRecordStore(cfg.RecordsFile)
	if err != nil {
		return nil, err
	}

	blacklistStore, err := storage.NewBlacklistStore(cfg.BlacklistFile)
	if err != nil {
		return nil, err
	}

	settingsStore, err := storage.NewSettingsStore(cfg.SettingsFile, storage.RuntimeSettings{
		MonitoringEnabled: true,
		MonitorChatIDs:    mapKeys(cfg.MonitorChatIDs),
		AlertChatID:       cfg.AlertChatID,
		CooldownMinutes:   int(cfg.Cooldown / time.Minute),
		MaxMessageLength:  100,
		FilterNoUsername:  true,
		FilterNoAvatar:    false,
		MinAccountAgeDays: 7,
		DMTemplate:        cfg.DMTemplate,
		DryRun:            cfg.DryRun,
	})
	if err != nil {
		return nil, err
	}

	accountStore, err := storage.NewMonitorAccountStore(cfg.AccountsFile, cfg.SessionsDir)
	if err != nil {
		return nil, err
	}

	app := &App{
		cfg:             cfg,
		logger:          logger,
		keywordStore:    keywordStore,
		recordStore:     recordStore,
		settings:        settingsStore,
		blacklist:       blacklistStore,
		matcher:         matcher.NewKeywordMatcher(),
		ruleEngine:      rules.NewEngine(settingsStore, recordStore),
		accountStore:    accountStore,
		authCoordinator: &tg.AuthCoordinator{},
		startedMonitors: make(map[string]struct{}),
	}

	if cfg.BotToken != "" && cfg.AdminUserID != 0 {
		adminClient := tg.NewBotAPIClient(cfg.BotToken)
		adminSvc := service.NewAdminService(
			cfg.AdminUserID,
			adminClient,
			keywordStore,
			settingsStore,
			blacklistStore,
			app.authCoordinator,
			app,
			logger,
		)
		app.adminBot = adminClient
		app.adminPoller = listener.NewPoller(adminClient, cfg.PollTimeout, logger, func(ctx context.Context, update model.Update) error {
			_, err := adminSvc.HandleUpdate(ctx, update)
			return err
		})
	}

	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Infof("jtbot user session version started")
	if a.adminPoller != nil {
		a.logger.Infof("admin bot backend enabled")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.runMu.Lock()
	a.runCtx = runCtx
	a.runMu.Unlock()

	if err := a.startInitialMonitors(runCtx); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	if a.adminPoller != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			if err := a.adminPoller.Run(runCtx); err != nil && runCtx.Err() == nil {
				errCh <- err
			}
		}()
	}

	select {
	case <-ctx.Done():
		cancel()
		a.wg.Wait()
		return nil
	case err := <-errCh:
		cancel()
		a.wg.Wait()
		return err
	}
}

func (a *App) StartMonitorLogin(ctx context.Context, phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", errors.New("手机号不能为空")
	}

	a.runMu.RLock()
	runCtx := a.runCtx
	a.runMu.RUnlock()
	if runCtx == nil {
		return "", errors.New("程序还没进入运行状态，请稍后再试")
	}

	account, added, err := a.accountStore.Add(phone)
	if err != nil {
		return "", err
	}
	if account.Phone == "" {
		return "", errors.New("手机号不能为空")
	}

	if err := a.startMonitorAccount(runCtx, account); err != nil {
		return "", err
	}

	if added {
		return fmt.Sprintf("已创建监控号 %s，并开始登录流程。收到验证码或两步密码提示后，直接在这里发送即可。", account.Phone), nil
	}
	return fmt.Sprintf("监控号 %s 已存在，已尝试重新启动会话。", account.Phone), nil
}

func (a *App) MonitorSummary() string {
	accounts := a.accountStore.List()
	active, _ := a.MonitorCounts()

	if len(accounts) == 0 {
		return "监控账号: 0个\n还没有配置监控号。点击“登录监控账号”后，直接发送手机号即可。"
	}

	lines := make([]string, 0, len(accounts)+1)
	lines = append(lines, fmt.Sprintf("监控账号: 在线 %d / 共 %d", active, len(accounts)))
	for _, account := range accounts {
		lines = append(lines, "• "+account.Phone)
	}
	return strings.Join(lines, "\n")
}

func (a *App) MonitorCounts() (active int, total int) {
	accounts := a.accountStore.List()
	a.runMu.RLock()
	active = len(a.startedMonitors)
	a.runMu.RUnlock()
	return active, len(accounts)
}

func (a *App) startInitialMonitors(ctx context.Context) error {
	accounts := a.accountStore.List()
	if len(accounts) == 0 && strings.TrimSpace(a.cfg.Phone) != "" && a.adminPoller == nil {
		account, _, err := a.accountStore.Add(a.cfg.Phone)
		if err != nil {
			return err
		}
		accounts = []storage.MonitorAccount{account}
	}

	if len(accounts) == 0 {
		if a.adminPoller != nil {
			a.logger.Infof("no monitor accounts yet, waiting for admin login")
			return nil
		}
		return errors.New("no monitor account configured")
	}

	for _, account := range accounts {
		if err := a.startMonitorAccount(ctx, account); err != nil {
			a.logger.Errorf("start monitor %s failed: %v", account.Phone, err)
		}
	}
	return nil
}

func (a *App) startMonitorAccount(ctx context.Context, account storage.MonitorAccount) error {
	a.runMu.Lock()
	if _, exists := a.startedMonitors[account.Phone]; exists {
		a.runMu.Unlock()
		return nil
	}
	a.startedMonitors[account.Phone] = struct{}{}
	a.runMu.Unlock()

	var client *tg.Client
	if a.adminBot != nil && a.cfg.AdminUserID != 0 {
		authFlow := tg.NewAdminAuthWithCoordinator(account.Phone, a.cfg.AdminUserID, a.adminBot, a.authCoordinator, a.logger)
		client = tg.NewClientWithAuth(a.cfg.AppID, a.cfg.AppHash, account.Phone, account.SessionFile, authFlow, a.logger)
	} else {
		client = tg.NewClient(a.cfg.AppID, a.cfg.AppHash, account.Phone, account.SessionFile, a.logger)
	}

	monitorCtx, cancel := context.WithCancel(ctx)
	jobQueue := queue.NewMessageQueue(a.cfg.QueueSize)
	dmSender := sender.New(client, a.recordStore, a.settings, a.logger)
	triggerSvc := service.NewTriggerService(
		a.matcher,
		a.keywordStore,
		a.ruleEngine,
		jobQueue,
		a.recordStore,
		client,
		a.adminBot,
		a.settings,
		a.blacklist,
		account.Phone,
		a.logger,
	)

	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		dmSender.Run(monitorCtx, jobQueue.Consume())
	}()

	go func() {
		defer a.wg.Done()
		defer cancel()
		defer a.markMonitorStopped(account.Phone)

		if err := client.Run(monitorCtx, triggerSvc.HandleUpdate); err != nil && monitorCtx.Err() == nil {
			a.logger.Errorf("monitor %s stopped: %v", account.Phone, err)
			a.notifyAdmin(monitorCtx, fmt.Sprintf("监控号 %s 启动失败：%v", account.Phone, err))
		}
	}()

	return nil
}

func (a *App) markMonitorStopped(phone string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	delete(a.startedMonitors, phone)
}

func (a *App) notifyAdmin(ctx context.Context, text string) {
	if a.adminBot == nil || a.cfg.AdminUserID == 0 || strings.TrimSpace(text) == "" {
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := a.adminBot.SendMessage(sendCtx, a.cfg.AdminUserID, text, nil); err != nil {
		a.logger.Errorf("notify admin failed: %v", err)
	}
}

func mapKeys(input map[int64]struct{}) []int64 {
	result := make([]int64, 0, len(input))
	for id := range input {
		result = append(result, id)
	}
	return result
}

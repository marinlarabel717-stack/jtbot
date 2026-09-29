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

type monitorRuntime struct {
	cancel      context.CancelFunc
	client      *tg.Client
	sessionFile string
	online      bool
	lastError   string
}

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
	dmStore      *storage.DMAccountStore

	authCoordinator *tg.AuthCoordinator

	runMu      sync.RWMutex
	runCtx     context.Context
	monitors   map[string]*monitorRuntime
	dmAccounts map[string]*monitorRuntime
	dmIndex    int
	wg         sync.WaitGroup
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
	if err := os.MkdirAll(cfg.DMSessionsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create dm sessions dir: %w", err)
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
	dmStore, err := storage.NewDMAccountStore(cfg.DMAccountsFile, cfg.DMSessionsDir)
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
		dmStore:         dmStore,
		authCoordinator: &tg.AuthCoordinator{},
		monitors:        make(map[string]*monitorRuntime),
		dmAccounts:      make(map[string]*monitorRuntime),
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
			app,
			recordStore,
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
	if err := a.startInitialDMAccounts(runCtx); err != nil {
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
		a.stopAllMonitors()
		a.stopAllDMAccounts()
		a.wg.Wait()
		return nil
	case err := <-errCh:
		cancel()
		a.stopAllMonitors()
		a.stopAllDMAccounts()
		a.wg.Wait()
		return err
	}
}

func (a *App) StartMonitorLogin(ctx context.Context, phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", errors.New("手机号不能为空")
	}

	runCtx, err := a.currentRunContext()
	if err != nil {
		return "", err
	}

	account, added, err := a.accountStore.Add(phone)
	if err != nil {
		return "", err
	}
	if account.Phone == "" {
		return "", errors.New("手机号不能为空")
	}

	if !added {
		if err := a.RestartMonitor(ctx, account.Phone); err != nil {
			return "", err
		}
		return fmt.Sprintf("监控号 %s 已存在，已重新发起登录/重连。", account.Phone), nil
	}

	if err := a.startMonitorAccount(runCtx, account); err != nil {
		return "", err
	}
	return fmt.Sprintf("已添加监控号 %s，并开始登录流程。收到验证码或两步密码提示后，直接在这里发送即可。", account.Phone), nil
}

func (a *App) MonitorSummary() string {
	accounts := a.ListMonitorAccounts()
	if len(accounts) == 0 {
		return "监控账号: 0个\n还没有配置监控号。点击“添加新账号”后，直接发送手机号即可。"
	}

	lines := make([]string, 0, len(accounts)+1)
	active, total := a.MonitorCounts()
	lines = append(lines, fmt.Sprintf("监控账号: %d在线 / %d离线", active, total-active))
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

func (a *App) MonitorCounts() (active int, total int) {
	accounts := a.accountStore.List()

	a.runMu.RLock()
	defer a.runMu.RUnlock()

	for _, runtime := range a.monitors {
		if runtime != nil && runtime.online {
			active++
		}
	}
	return active, len(accounts)
}

func (a *App) ListMonitorAccounts() []service.MonitorAccountInfo {
	accounts := a.accountStore.List()
	result := make([]service.MonitorAccountInfo, 0, len(accounts))

	a.runMu.RLock()
	defer a.runMu.RUnlock()

	for _, account := range accounts {
		info := service.MonitorAccountInfo{
			Phone:       account.Phone,
			SessionFile: account.SessionFile,
		}
		if runtime, ok := a.monitors[account.Phone]; ok && runtime != nil {
			info.Online = runtime.online
			info.LastError = runtime.lastError
		}
		result = append(result, info)
	}
	return result
}

func (a *App) StartDMLogin(ctx context.Context, phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if phone == "" {
		return "", errors.New("手机号不能为空")
	}

	runCtx, err := a.currentRunContext()
	if err != nil {
		return "", err
	}

	account, added, err := a.dmStore.Add(phone)
	if err != nil {
		return "", err
	}
	if account.Phone == "" {
		return "", errors.New("手机号不能为空")
	}

	if !added {
		if err := a.RestartDMAccount(ctx, account.Phone); err != nil {
			return "", err
		}
		return fmt.Sprintf("私信号 %s 已存在，已重新发起登录/重连。", account.Phone), nil
	}

	if err := a.startDMAccount(runCtx, account); err != nil {
		return "", err
	}
	return fmt.Sprintf("已添加私信号 %s，并开始登录流程。收到验证码或两步密码提示后，直接在这里发送即可。", account.Phone), nil
}

func (a *App) ImportDMSessions(ctx context.Context, filename string, data []byte) (string, error) {
	files, err := tg.ExtractTelethonSessionFiles(filename, data)
	if err != nil {
		return "", err
	}

	runCtx, err := a.currentRunContext()
	if err != nil {
		return "", err
	}

	imported := 0
	failed := 0
	details := make([]string, 0, len(files))
	for _, file := range files {
		phone, err := a.importSingleDMSession(ctx, runCtx, file)
		if err != nil {
			failed++
			details = append(details, fmt.Sprintf("- %s: %v", file.Name, err))
			continue
		}
		imported++
		details = append(details, fmt.Sprintf("- %s -> %s", file.Name, phone))
	}

	lines := []string{
		fmt.Sprintf("Session 导入完成，共 %d 个文件。", len(files)),
		fmt.Sprintf("成功: %d", imported),
		fmt.Sprintf("失败: %d", failed),
	}
	if len(details) > 0 {
		limit := len(details)
		if limit > 8 {
			limit = 8
		}
		lines = append(lines, "", strings.Join(details[:limit], "\n"))
		if len(details) > limit {
			lines = append(lines, fmt.Sprintf("… 另外还有 %d 条结果未展开", len(details)-limit))
		}
	}
	return strings.Join(lines, "\n"), nil
}

func (a *App) DMCounts() (active int, total int) {
	accounts := a.dmStore.List()

	a.runMu.RLock()
	defer a.runMu.RUnlock()

	for _, runtime := range a.dmAccounts {
		if runtime != nil && runtime.online {
			active++
		}
	}
	return active, len(accounts)
}

func (a *App) ListDMAccounts() []service.DMAccountInfo {
	accounts := a.dmStore.List()
	result := make([]service.DMAccountInfo, 0, len(accounts))

	a.runMu.RLock()
	defer a.runMu.RUnlock()

	for _, account := range accounts {
		info := service.DMAccountInfo{
			Phone:       account.Phone,
			SessionFile: account.SessionFile,
		}
		if runtime, ok := a.dmAccounts[account.Phone]; ok && runtime != nil {
			info.Online = runtime.online
			info.LastError = runtime.lastError
		}
		result = append(result, info)
	}
	return result
}

func (a *App) GetDMAccount(phone string) (service.DMAccountInfo, bool) {
	for _, account := range a.ListDMAccounts() {
		if account.Phone == strings.TrimSpace(phone) {
			return account, true
		}
	}
	return service.DMAccountInfo{}, false
}

func (a *App) importSingleDMSession(ctx, runCtx context.Context, file tg.ImportedSessionFile) (string, error) {
	tmpDir, err := os.MkdirTemp("", "jtbot-dm-import-*")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	sqlitePath := filepath.Join(tmpDir, filepath.Base(file.Name))
	if err := os.WriteFile(sqlitePath, file.Data, 0o600); err != nil {
		return "", fmt.Errorf("write temp session: %w", err)
	}

	tmpSessionPath := filepath.Join(tmpDir, "imported.json")
	if err := tg.ConvertTelethonSQLiteSessionFile(ctx, sqlitePath, tmpSessionPath); err != nil {
		return "", err
	}

	probeCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	probeClient := tg.NewClientWithAuth(a.cfg.AppID, a.cfg.AppHash, "", tmpSessionPath, nil, a.logger)
	self, err := probeClient.ProbeSession(probeCtx)
	if err != nil {
		return "", err
	}

	phone := strings.TrimSpace(self.Phone)
	if phone == "" {
		phone = a.extractPhoneFromImportedSessionName(file.Name)
	}
	if strings.TrimSpace(phone) == "" {
		phone = fmt.Sprintf("user_%d", self.ID)
	}

	account, added, err := a.dmStore.Add(phone)
	if err != nil {
		return "", err
	}
	if account.Phone == "" {
		return "", errors.New("导入后的私信号标识为空")
	}

	if err := tg.ConvertTelethonSQLiteSessionFile(ctx, sqlitePath, account.SessionFile); err != nil {
		return "", err
	}

	if added {
		if err := a.startDMAccount(runCtx, account); err != nil {
			return "", err
		}
	} else {
		if err := a.RestartDMAccount(ctx, account.Phone); err != nil {
			return "", err
		}
	}

	return account.Phone, nil
}

func (a *App) extractPhoneFromImportedSessionName(name string) string {
	base := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
	base = strings.TrimSpace(base)
	if base == "" {
		return ""
	}

	hasPlus := strings.Contains(base, "+")
	var digits strings.Builder
	for _, r := range base {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	if digits.Len() < 6 {
		return ""
	}
	if hasPlus {
		return "+" + digits.String()
	}
	return digits.String()
}

func (a *App) RestartDMAccount(ctx context.Context, phone string) error {
	account, ok := a.dmStore.Get(phone)
	if !ok {
		return errors.New("私信号不存在")
	}

	runCtx, err := a.currentRunContext()
	if err != nil {
		return err
	}

	a.stopDMAccount(phone)
	return a.startDMAccount(runCtx, account)
}

func (a *App) DeleteDMAccount(ctx context.Context, phone string) error {
	account, removed, err := a.dmStore.Remove(phone)
	if err != nil {
		return err
	}
	if !removed {
		return errors.New("私信号不存在")
	}

	a.stopDMAccount(phone)

	if strings.TrimSpace(account.SessionFile) != "" {
		if err := os.Remove(account.SessionFile); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (a *App) GetMonitorAccount(phone string) (service.MonitorAccountInfo, bool) {
	for _, account := range a.ListMonitorAccounts() {
		if account.Phone == strings.TrimSpace(phone) {
			return account, true
		}
	}
	return service.MonitorAccountInfo{}, false
}

func (a *App) RestartMonitor(ctx context.Context, phone string) error {
	account, ok := a.accountStore.Get(phone)
	if !ok {
		return errors.New("监控号不存在")
	}

	runCtx, err := a.currentRunContext()
	if err != nil {
		return err
	}

	a.stopMonitor(phone)
	return a.startMonitorAccount(runCtx, account)
}

func (a *App) DeleteMonitor(ctx context.Context, phone string) error {
	account, removed, err := a.accountStore.Remove(phone)
	if err != nil {
		return err
	}
	if !removed {
		return errors.New("监控号不存在")
	}

	a.stopMonitor(phone)

	if strings.TrimSpace(account.SessionFile) != "" {
		if err := os.Remove(account.SessionFile); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
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

func (a *App) startInitialDMAccounts(ctx context.Context) error {
	for _, account := range a.dmStore.List() {
		if err := a.startDMAccount(ctx, account); err != nil {
			a.logger.Errorf("start dm account %s failed: %v", account.Phone, err)
		}
	}
	return nil
}

func (a *App) startMonitorAccount(ctx context.Context, account storage.MonitorAccount) error {
	a.runMu.Lock()
	if existing, exists := a.monitors[account.Phone]; exists && existing != nil && existing.online {
		a.runMu.Unlock()
		return nil
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	a.monitors[account.Phone] = &monitorRuntime{
		cancel:      cancel,
		sessionFile: account.SessionFile,
		online:      true,
	}
	a.runMu.Unlock()

	var client *tg.Client
	if a.adminBot != nil && a.cfg.AdminUserID != 0 {
		authFlow := tg.NewAdminAuthWithCoordinator(account.Phone, a.cfg.AdminUserID, a.adminBot, a.authCoordinator, a.logger)
		client = tg.NewClientWithAuth(a.cfg.AppID, a.cfg.AppHash, account.Phone, account.SessionFile, authFlow, a.logger)
	} else {
		client = tg.NewClient(a.cfg.AppID, a.cfg.AppHash, account.Phone, account.SessionFile, a.logger)
	}
	a.runMu.Lock()
	if runtime := a.monitors[account.Phone]; runtime != nil {
		runtime.client = client
	}
	a.runMu.Unlock()

	jobQueue := queue.NewMessageQueue(a.cfg.QueueSize)
	dmSender := sender.New(client, a, a.recordStore, a.settings, a.logger)
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

		err := client.Run(monitorCtx, triggerSvc.HandleUpdate)
		if err != nil && monitorCtx.Err() == nil {
			a.setMonitorError(account.Phone, err.Error())
			a.logger.Errorf("monitor %s stopped: %v", account.Phone, err)
			a.notifyAdmin(monitorCtx, fmt.Sprintf("监控号 %s 启动失败：%v", account.Phone, err))
		} else {
			a.clearMonitorError(account.Phone)
		}
		a.markMonitorStopped(account.Phone)
	}()

	return nil
}

func (a *App) startDMAccount(ctx context.Context, account storage.DMAccount) error {
	a.runMu.Lock()
	if existing, exists := a.dmAccounts[account.Phone]; exists && existing != nil && existing.online {
		a.runMu.Unlock()
		return nil
	}
	dmCtx, cancel := context.WithCancel(ctx)
	a.dmAccounts[account.Phone] = &monitorRuntime{
		cancel:      cancel,
		sessionFile: account.SessionFile,
		online:      true,
	}
	a.runMu.Unlock()

	var client *tg.Client
	if a.adminBot != nil && a.cfg.AdminUserID != 0 {
		authFlow := tg.NewAdminAuthWithCoordinator(account.Phone, a.cfg.AdminUserID, a.adminBot, a.authCoordinator, a.logger)
		client = tg.NewClientWithAuth(a.cfg.AppID, a.cfg.AppHash, account.Phone, account.SessionFile, authFlow, a.logger)
	} else {
		client = tg.NewClient(a.cfg.AppID, a.cfg.AppHash, account.Phone, account.SessionFile, a.logger)
	}
	a.runMu.Lock()
	if runtime := a.dmAccounts[account.Phone]; runtime != nil {
		runtime.client = client
	}
	a.runMu.Unlock()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer cancel()

		err := client.Run(dmCtx, func(context.Context, model.Update) error { return nil })
		if err != nil && dmCtx.Err() == nil {
			a.setDMAccountError(account.Phone, err.Error())
			a.logger.Errorf("dm account %s stopped: %v", account.Phone, err)
			a.notifyAdmin(dmCtx, fmt.Sprintf("私信号 %s 启动失败：%v", account.Phone, err))
		} else {
			a.clearDMAccountError(account.Phone)
		}
		a.markDMAccountStopped(account.Phone)
	}()

	return nil
}

func (a *App) currentRunContext() (context.Context, error) {
	a.runMu.RLock()
	defer a.runMu.RUnlock()
	if a.runCtx == nil {
		return nil, errors.New("程序还没进入运行状态，请稍后再试")
	}
	return a.runCtx, nil
}

func (a *App) stopMonitor(phone string) {
	a.runMu.Lock()
	runtime := a.monitors[phone]
	if runtime != nil {
		runtime.online = false
	}
	a.runMu.Unlock()
	if runtime != nil && runtime.cancel != nil {
		runtime.cancel()
	}
}

func (a *App) stopAllMonitors() {
	a.runMu.RLock()
	phones := make([]string, 0, len(a.monitors))
	for phone := range a.monitors {
		phones = append(phones, phone)
	}
	a.runMu.RUnlock()

	for _, phone := range phones {
		a.stopMonitor(phone)
	}
}

func (a *App) stopDMAccount(phone string) {
	a.runMu.Lock()
	runtime := a.dmAccounts[phone]
	if runtime != nil {
		runtime.online = false
	}
	a.runMu.Unlock()
	if runtime != nil && runtime.cancel != nil {
		runtime.cancel()
	}
}

func (a *App) stopAllDMAccounts() {
	a.runMu.RLock()
	phones := make([]string, 0, len(a.dmAccounts))
	for phone := range a.dmAccounts {
		phones = append(phones, phone)
	}
	a.runMu.RUnlock()

	for _, phone := range phones {
		a.stopDMAccount(phone)
	}
}

func (a *App) markMonitorStopped(phone string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if runtime, ok := a.monitors[phone]; ok && runtime != nil {
		runtime.online = false
	}
}

func (a *App) markDMAccountStopped(phone string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if runtime, ok := a.dmAccounts[phone]; ok && runtime != nil {
		runtime.online = false
	}
}

func (a *App) setMonitorError(phone, text string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	runtime, ok := a.monitors[phone]
	if !ok || runtime == nil {
		runtime = &monitorRuntime{}
		a.monitors[phone] = runtime
	}
	runtime.online = false
	runtime.lastError = strings.TrimSpace(text)
}

func (a *App) setDMAccountError(phone, text string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	runtime, ok := a.dmAccounts[phone]
	if !ok || runtime == nil {
		runtime = &monitorRuntime{}
		a.dmAccounts[phone] = runtime
	}
	runtime.online = false
	runtime.lastError = strings.TrimSpace(text)
}

func (a *App) clearMonitorError(phone string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if runtime, ok := a.monitors[phone]; ok && runtime != nil {
		runtime.lastError = ""
	}
}

func (a *App) clearDMAccountError(phone string) {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	if runtime, ok := a.dmAccounts[phone]; ok && runtime != nil {
		runtime.lastError = ""
	}
}

func (a *App) SendDM(ctx context.Context, _ *tg.Client, job model.DMJob, text string) (string, error) {
	a.runMu.Lock()
	accounts := a.dmStore.List()
	available := make([]string, 0, len(accounts))
	for _, account := range accounts {
		runtime := a.dmAccounts[account.Phone]
		if runtime != nil && runtime.online {
			available = append(available, account.Phone)
		}
	}

	var selectedPhone string
	var selectedClient *tg.Client
	if len(available) > 0 {
		selectedPhone = available[a.dmIndex%len(available)]
		a.dmIndex = (a.dmIndex + 1) % len(available)
		if runtime := a.dmAccounts[selectedPhone]; runtime != nil {
			selectedClient = runtime.client
		}
	}
	a.runMu.Unlock()

	if selectedPhone != "" && selectedClient != nil {
		if err := selectedClient.SendDirectMessage(ctx, job.TargetUserID, job.Username, text); err == nil {
			return selectedPhone, nil
		}
		return selectedPhone, fmt.Errorf("私信号 %s 发送失败", selectedPhone)
	}

	return "", errors.New("没有可用的私信发送账号，请先在私信号池添加或上传私信号")
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

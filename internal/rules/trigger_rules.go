package rules

import (
	"strconv"
	"sync"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
)

type Engine struct {
	settings    *storage.SettingsStore
	recordStore *storage.RecordStore

	mu            sync.Mutex
	processedMsgs map[string]time.Time
}

func NewEngine(settings *storage.SettingsStore, recordStore *storage.RecordStore) *Engine {
	return &Engine{
		settings:      settings,
		recordStore:   recordStore,
		processedMsgs: make(map[string]time.Time),
	}
}

func (e *Engine) AllowChat(chatID int64) bool {
	if !e.settings.IsMonitoringEnabled() {
		return false
	}
	monitorChatIDs := e.settings.MonitorChatIDSet()
	if len(monitorChatIDs) == 0 {
		return true
	}
	_, ok := monitorChatIDs[chatID]
	return ok
}

func (e *Engine) MarkProcessed(chatID, messageID int64) bool {
	key := strconv.FormatInt(chatID, 10) + ":" + strconv.FormatInt(messageID, 10)
	now := time.Now()

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.processedMsgs[key]; exists {
		return false
	}
	e.processedMsgs[key] = now

	cutoff := now.Add(-10 * time.Minute)
	for existingKey, seenAt := range e.processedMsgs {
		if seenAt.Before(cutoff) {
			delete(e.processedMsgs, existingKey)
		}
	}
	return true
}

func (e *Engine) CanQueueDM(job model.DMJob) bool {
	return e.ExplainDMBlockReason(job) == ""
}

func (e *Engine) ExplainDMBlockReason(job model.DMJob) string {
	if e.recordStore.IsUserInCooldown(job.TargetUserID, e.settings.Cooldown()) {
		return "同一个用户还在重复私信冷却时间内，这次先不再私信"
	}
	if e.recordStore.IsUserInChatCooldown(job.TargetUserID, job.ChatID, e.settings.ChatCooldown()) {
		return "这个用户在当前群最近已经私信过了，这次先跳过"
	}
	if e.recordStore.IsUserTextInCooldown(job.TargetUserID, job.ChatID, job.SourceText, e.settings.TextCooldown()) {
		return "这个用户最近发过相同内容，命中同内容冷却，这次先跳过"
	}
	return ""
}

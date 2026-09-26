package rules

import (
	"strconv"
	"sync"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/storage"
)

type Engine struct {
	monitorChatIDs map[int64]struct{}
	cooldown       time.Duration
	recordStore    *storage.RecordStore

	mu            sync.Mutex
	processedMsgs map[string]time.Time
}

func NewEngine(monitorChatIDs map[int64]struct{}, cooldown time.Duration, recordStore *storage.RecordStore) *Engine {
	return &Engine{
		monitorChatIDs: monitorChatIDs,
		cooldown:       cooldown,
		recordStore:    recordStore,
		processedMsgs:  make(map[string]time.Time),
	}
}

func (e *Engine) AllowChat(chatID int64) bool {
	if len(e.monitorChatIDs) == 0 {
		return true
	}
	_, ok := e.monitorChatIDs[chatID]
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

func (e *Engine) CanQueueDM(userID int64) bool {
	return !e.recordStore.IsUserInCooldown(userID, e.cooldown)
}

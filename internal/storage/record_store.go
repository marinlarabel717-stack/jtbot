package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type RecordStore struct {
	path string

	mu    sync.Mutex
	state recordState
}

type recordState struct {
	MatchRecords []model.MatchRecord  `json:"match_records"`
	DMRecords    []model.DMRecord     `json:"dm_records"`
	LastSent     map[string]time.Time `json:"last_sent"`
}

func NewRecordStore(path string) (*RecordStore, error) {
	store := &RecordStore{
		path: path,
		state: recordState{
			MatchRecords: make([]model.MatchRecord, 0),
			DMRecords:    make([]model.DMRecord, 0),
			LastSent:     make(map[string]time.Time),
		},
	}

	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *RecordStore) SaveMatchRecord(record model.MatchRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state.MatchRecords = append(s.state.MatchRecords, record)
	_ = s.persist()
}

func (s *RecordStore) SaveDMRecord(record model.DMRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state.DMRecords = append(s.state.DMRecords, record)
	if record.Status == "sent" || record.Status == "dry_run" {
		s.state.LastSent[strconv.FormatInt(record.UserID, 10)] = record.SentAt
	}
	_ = s.persist()
}

func (s *RecordStore) IsUserInCooldown(userID int64, cooldown time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastSent, ok := s.state.LastSent[strconv.FormatInt(userID, 10)]
	if !ok {
		return false
	}
	return time.Since(lastSent) < cooldown
}

func (s *RecordStore) IsUserInChatCooldown(userID, chatID int64, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-cooldown)
	for i := len(s.state.DMRecords) - 1; i >= 0; i-- {
		record := s.state.DMRecords[i]
		if record.SentAt.Before(cutoff) {
			break
		}
		if record.UserID == userID && record.ChatID == chatID && isSuccessfulDM(record.Status) {
			return true
		}
	}
	return false
}

func (s *RecordStore) IsUserTextInCooldown(userID, chatID int64, sourceText string, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return false
	}
	_ = chatID

	sourceText = normalizeSourceText(sourceText)
	if sourceText == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-cooldown)
	for i := len(s.state.DMRecords) - 1; i >= 0; i-- {
		record := s.state.DMRecords[i]
		if record.SentAt.Before(cutoff) {
			break
		}
		if record.UserID == userID && isSuccessfulDM(record.Status) && normalizeSourceText(record.SourceText) == sourceText {
			return true
		}
	}
	return false
}

func (s *RecordStore) MatchRecords() []model.MatchRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]model.MatchRecord, len(s.state.MatchRecords))
	copy(records, s.state.MatchRecords)
	return records
}

func (s *RecordStore) DMRecords() []model.DMRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]model.DMRecord, len(s.state.DMRecords))
	copy(records, s.state.DMRecords)
	return records
}

func (s *RecordStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return s.persist()
		}
		return fmt.Errorf("read records file: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	if err := json.Unmarshal(data, &s.state); err != nil {
		return fmt.Errorf("parse records file: %w", err)
	}
	if s.state.LastSent == nil {
		s.state.LastSent = make(map[string]time.Time)
	}
	return nil
}

func (s *RecordStore) persist() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.path)
}

func isSuccessfulDM(status string) bool {
	return status == "sent" || status == "dry_run"
}

func normalizeSourceText(text string) string {
	return strings.TrimSpace(strings.ToLower(text))
}

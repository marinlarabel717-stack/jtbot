package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type SettingsStore struct {
	path  string
	mu    sync.RWMutex
	state RuntimeSettings
}

type RuntimeSettings struct {
	MonitoringEnabled bool    `json:"monitoring_enabled"`
	MonitorChatIDs    []int64 `json:"monitor_chat_ids"`
	AlertChatID       int64   `json:"alert_chat_id"`
	CooldownMinutes   int     `json:"cooldown_minutes"`
	DMTemplate        string  `json:"dm_template"`
	DryRun            bool    `json:"dry_run"`
}

func NewSettingsStore(path string, defaults RuntimeSettings) (*SettingsStore, error) {
	store := &SettingsStore{
		path:  path,
		state: defaults,
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *SettingsStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return s.persistLocked()
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}

	var loaded RuntimeSettings
	if err := json.Unmarshal(data, &loaded); err != nil {
		return err
	}

	s.state.MonitoringEnabled = loaded.MonitoringEnabled
	s.state.MonitorChatIDs = normalizeChatIDs(loaded.MonitorChatIDs)
	s.state.AlertChatID = loaded.AlertChatID
	if loaded.CooldownMinutes > 0 {
		s.state.CooldownMinutes = loaded.CooldownMinutes
	}
	if loaded.DMTemplate != "" {
		s.state.DMTemplate = loaded.DMTemplate
	}
	s.state.DryRun = loaded.DryRun
	return nil
}

func (s *SettingsStore) Snapshot() RuntimeSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.clone()
}

func (s *SettingsStore) IsMonitoringEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.MonitoringEnabled
}

func (s *SettingsStore) ToggleMonitoring() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.MonitoringEnabled = !s.state.MonitoringEnabled
	return s.state.MonitoringEnabled, s.persistLocked()
}

func (s *SettingsStore) MonitorChatIDSet() map[int64]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[int64]struct{}, len(s.state.MonitorChatIDs))
	for _, id := range s.state.MonitorChatIDs {
		result[id] = struct{}{}
	}
	return result
}

func (s *SettingsStore) AddMonitorChats(ids []int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := make(map[int64]struct{}, len(s.state.MonitorChatIDs))
	for _, id := range s.state.MonitorChatIDs {
		existing[id] = struct{}{}
	}

	added := 0
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := existing[id]; ok {
			continue
		}
		s.state.MonitorChatIDs = append(s.state.MonitorChatIDs, id)
		existing[id] = struct{}{}
		added++
	}
	s.state.MonitorChatIDs = normalizeChatIDs(s.state.MonitorChatIDs)
	if added == 0 {
		return 0, nil
	}
	return added, s.persistLocked()
}

func (s *SettingsStore) RemoveMonitorChats(ids []int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	toDelete := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		toDelete[id] = struct{}{}
	}
	filtered := make([]int64, 0, len(s.state.MonitorChatIDs))
	removed := 0
	for _, id := range s.state.MonitorChatIDs {
		if _, ok := toDelete[id]; ok {
			removed++
			continue
		}
		filtered = append(filtered, id)
	}
	s.state.MonitorChatIDs = filtered
	if removed == 0 {
		return 0, nil
	}
	return removed, s.persistLocked()
}

func (s *SettingsStore) AlertChatID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.AlertChatID
}

func (s *SettingsStore) SetAlertChatID(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.AlertChatID = id
	return s.persistLocked()
}

func (s *SettingsStore) Cooldown() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Duration(s.state.CooldownMinutes) * time.Minute
}

func (s *SettingsStore) SetCooldownMinutes(minutes int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.CooldownMinutes = minutes
	return s.persistLocked()
}

func (s *SettingsStore) DMTemplate() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.DMTemplate
}

func (s *SettingsStore) SetDMTemplate(template string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.DMTemplate = template
	return s.persistLocked()
}

func (s *SettingsStore) IsDryRun() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.DryRun
}

func (s *SettingsStore) ToggleDryRun() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.DryRun = !s.state.DryRun
	return s.state.DryRun, s.persistLocked()
}

func (s *SettingsStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o644)
}

func (r RuntimeSettings) clone() RuntimeSettings {
	r.MonitorChatIDs = append([]int64(nil), r.MonitorChatIDs...)
	return r
}

func normalizeChatIDs(ids []int64) []int64 {
	set := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id != 0 {
			set[id] = struct{}{}
		}
	}
	result := make([]int64, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

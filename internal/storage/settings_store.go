package storage

import (
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type SettingsStore struct {
	path  string
	mu    sync.RWMutex
	state RuntimeSettings
}

type RuntimeSettings struct {
	MonitoringEnabled bool     `json:"monitoring_enabled"`
	MonitorChatIDs    []int64  `json:"monitor_chat_ids"`
	AlertChatID       int64    `json:"alert_chat_id"`
	CooldownMinutes   int      `json:"cooldown_minutes"`
	MaxMessageLength  int      `json:"max_message_length"`
	FilterNoUsername  bool     `json:"filter_no_username"`
	FilterNoAvatar    bool     `json:"filter_no_avatar"`
	MinAccountAgeDays int      `json:"min_account_age_days"`
	DMTemplate        string   `json:"dm_template"`
	DMTemplates       []string `json:"dm_templates"`
	DryRun            bool     `json:"dry_run"`
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

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if value, ok := raw["monitoring_enabled"]; ok {
		_ = json.Unmarshal(value, &s.state.MonitoringEnabled)
	}
	if value, ok := raw["monitor_chat_ids"]; ok {
		var ids []int64
		if err := json.Unmarshal(value, &ids); err == nil {
			s.state.MonitorChatIDs = normalizeChatIDs(ids)
		}
	}
	if value, ok := raw["alert_chat_id"]; ok {
		_ = json.Unmarshal(value, &s.state.AlertChatID)
	}
	if value, ok := raw["cooldown_minutes"]; ok {
		var minutes int
		if err := json.Unmarshal(value, &minutes); err == nil && minutes > 0 {
			s.state.CooldownMinutes = minutes
		}
	}
	if value, ok := raw["max_message_length"]; ok {
		var maxLength int
		if err := json.Unmarshal(value, &maxLength); err == nil && maxLength > 0 {
			s.state.MaxMessageLength = maxLength
		}
	}
	if value, ok := raw["filter_no_username"]; ok {
		_ = json.Unmarshal(value, &s.state.FilterNoUsername)
	}
	if value, ok := raw["filter_no_avatar"]; ok {
		_ = json.Unmarshal(value, &s.state.FilterNoAvatar)
	}
	if value, ok := raw["min_account_age_days"]; ok {
		var days int
		if err := json.Unmarshal(value, &days); err == nil && days > 0 {
			s.state.MinAccountAgeDays = days
		}
	}
	if value, ok := raw["dm_template"]; ok {
		var template string
		if err := json.Unmarshal(value, &template); err == nil && template != "" {
			s.state.DMTemplate = template
		}
	}
	if value, ok := raw["dm_templates"]; ok {
		var templates []string
		if err := json.Unmarshal(value, &templates); err == nil {
			s.state.DMTemplates = normalizeTemplates(templates)
		}
	}
	if value, ok := raw["dry_run"]; ok {
		_ = json.Unmarshal(value, &s.state.DryRun)
	}
	if len(s.state.DMTemplates) == 0 && strings.TrimSpace(s.state.DMTemplate) != "" {
		s.state.DMTemplates = []string{s.state.DMTemplate}
	}
	if strings.TrimSpace(s.state.DMTemplate) == "" && len(s.state.DMTemplates) > 0 {
		s.state.DMTemplate = s.state.DMTemplates[0]
	}
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

func (s *SettingsStore) MaxMessageLength() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.MaxMessageLength
}

func (s *SettingsStore) SetMaxMessageLength(length int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.MaxMessageLength = length
	return s.persistLocked()
}

func (s *SettingsStore) FilterNoUsername() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.FilterNoUsername
}

func (s *SettingsStore) ToggleFilterNoUsername() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.FilterNoUsername = !s.state.FilterNoUsername
	return s.state.FilterNoUsername, s.persistLocked()
}

func (s *SettingsStore) FilterNoAvatar() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.FilterNoAvatar
}

func (s *SettingsStore) ToggleFilterNoAvatar() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.FilterNoAvatar = !s.state.FilterNoAvatar
	return s.state.FilterNoAvatar, s.persistLocked()
}

func (s *SettingsStore) MinAccountAgeDays() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.MinAccountAgeDays
}

func (s *SettingsStore) SetMinAccountAgeDays(days int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.MinAccountAgeDays = days
	return s.persistLocked()
}

func (s *SettingsStore) SetDMTemplate(template string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	template = strings.TrimSpace(template)
	s.state.DMTemplate = template
	if template != "" {
		templates := []string{template}
		for _, item := range s.state.DMTemplates {
			item = strings.TrimSpace(item)
			if item == "" || item == template {
				continue
			}
			templates = append(templates, item)
		}
		s.state.DMTemplates = templates
	}
	return s.persistLocked()
}

func (s *SettingsStore) ListDMTemplates() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.state.DMTemplates) == 0 && strings.TrimSpace(s.state.DMTemplate) != "" {
		return []string{s.state.DMTemplate}
	}
	return append([]string(nil), s.state.DMTemplates...)
}

func (s *SettingsStore) AddDMTemplates(templates []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := make(map[string]struct{}, len(s.state.DMTemplates))
	for _, item := range s.state.DMTemplates {
		item = strings.TrimSpace(item)
		if item != "" {
			existing[item] = struct{}{}
		}
	}

	added := 0
	for _, item := range normalizeTemplates(templates) {
		if _, ok := existing[item]; ok {
			continue
		}
		s.state.DMTemplates = append(s.state.DMTemplates, item)
		existing[item] = struct{}{}
		added++
	}
	if strings.TrimSpace(s.state.DMTemplate) == "" && len(s.state.DMTemplates) > 0 {
		s.state.DMTemplate = s.state.DMTemplates[0]
	}
	if added == 0 {
		return 0, nil
	}
	return added, s.persistLocked()
}

func (s *SettingsStore) RemoveDMTemplates(templates []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	toDelete := make(map[string]struct{}, len(templates))
	for _, item := range normalizeTemplates(templates) {
		toDelete[item] = struct{}{}
	}

	filtered := make([]string, 0, len(s.state.DMTemplates))
	removed := 0
	for _, item := range s.state.DMTemplates {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := toDelete[item]; ok {
			removed++
			continue
		}
		filtered = append(filtered, item)
	}
	s.state.DMTemplates = filtered
	if len(filtered) == 0 {
		s.state.DMTemplate = ""
	} else if _, ok := toDelete[s.state.DMTemplate]; ok || strings.TrimSpace(s.state.DMTemplate) == "" {
		s.state.DMTemplate = filtered[0]
	}
	if removed == 0 {
		return 0, nil
	}
	return removed, s.persistLocked()
}

func (s *SettingsStore) RandomDMTemplate() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	templates := s.state.DMTemplates
	if len(templates) == 0 && strings.TrimSpace(s.state.DMTemplate) != "" {
		return s.state.DMTemplate
	}
	if len(templates) == 0 {
		return ""
	}
	return templates[rand.Intn(len(templates))]
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
	r.DMTemplates = append([]string(nil), r.DMTemplates...)
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

func normalizeTemplates(templates []string) []string {
	result := make([]string, 0, len(templates))
	seen := make(map[string]struct{}, len(templates))
	for _, item := range templates {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	return result
}

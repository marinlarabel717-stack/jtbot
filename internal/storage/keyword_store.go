package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type KeywordStore struct {
	path string
	mu   sync.RWMutex
	data keywordFile
}

type keywordFile struct {
	Keywords []string            `json:"keywords,omitempty"`
	Entries  []model.KeywordRule `json:"entries,omitempty"`
}

type keywordDeleteSelector struct {
	text string
	mode string
}

func NewKeywordStore(path string) *KeywordStore {
	return &KeywordStore{path: path}
}

func (s *KeywordStore) Load() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("read keywords file: %w", err)
	}

	if err := json.Unmarshal(data, &s.data); err != nil {
		return nil, fmt.Errorf("parse keywords file: %w", err)
	}
	s.data.Entries = normalizeKeywordRules(s.data.Entries, s.data.Keywords)
	result := make([]string, 0, len(s.data.Entries))
	for _, entry := range s.data.Entries {
		result = append(result, entry.Text)
	}
	return result, nil
}

func (s *KeywordStore) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.data.Entries))
	for _, entry := range s.data.Entries {
		result = append(result, entry.Text)
	}
	return result
}

func (s *KeywordStore) ListEntries() []model.KeywordRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]model.KeywordRule(nil), s.data.Entries...)
}

func (s *KeywordStore) Add(raw []string) (int, error) {
	return s.AddWithMode("fuzzy", raw)
}

func (s *KeywordStore) AddWithMode(mode string, raw []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	mode = normalizeKeywordMode(mode)
	existing := make(map[string]struct{}, len(s.data.Entries))
	for _, keyword := range s.data.Entries {
		existing[keywordRuleKey(keyword.Mode, keyword.Text)] = struct{}{}
	}

	added := 0
	for _, keyword := range raw {
		keyword = strings.TrimSpace(keyword)
		if keyword == "" {
			continue
		}
		key := keywordRuleKey(mode, keyword)
		if _, ok := existing[key]; ok {
			continue
		}
		s.data.Entries = append(s.data.Entries, model.KeywordRule{Text: keyword, Mode: mode})
		existing[key] = struct{}{}
		added++
	}
	if added == 0 {
		return 0, nil
	}
	return added, s.persistLocked()
}

func (s *KeywordStore) Remove(raw []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	toDelete := make([]keywordDeleteSelector, 0, len(raw))
	for _, keyword := range raw {
		text, mode := parseKeywordSelector(keyword)
		if text != "" {
			toDelete = append(toDelete, keywordDeleteSelector{text: strings.ToLower(text), mode: mode})
		}
	}

	filtered := make([]model.KeywordRule, 0, len(s.data.Entries))
	removed := 0
	for _, keyword := range s.data.Entries {
		if shouldDeleteKeywordRule(keyword, toDelete) {
			removed++
			continue
		}
		filtered = append(filtered, keyword)
	}
	s.data.Entries = filtered
	if removed == 0 {
		return 0, nil
	}
	return removed, s.persistLocked()
}

func (s *KeywordStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	s.data.Entries = normalizeKeywordRules(s.data.Entries, nil)
	s.data.Keywords = nil
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o644)
}

func normalizeKeywordRules(entries []model.KeywordRule, legacy []string) []model.KeywordRule {
	if len(entries) == 0 && len(legacy) > 0 {
		entries = make([]model.KeywordRule, 0, len(legacy))
		for _, keyword := range legacy {
			entries = append(entries, model.KeywordRule{Text: keyword, Mode: "fuzzy"})
		}
	}

	result := make([]model.KeywordRule, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		text := strings.TrimSpace(entry.Text)
		if text == "" {
			continue
		}
		mode := normalizeKeywordMode(entry.Mode)
		key := keywordRuleKey(mode, text)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, model.KeywordRule{Text: text, Mode: mode})
	}
	return result
}

func normalizeKeywordMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "exact":
		return "exact"
	default:
		return "fuzzy"
	}
}

func keywordRuleKey(mode, text string) string {
	return normalizeKeywordMode(mode) + ":" + strings.ToLower(strings.TrimSpace(text))
}

func parseKeywordSelector(raw string) (text string, mode string) {
	value := strings.TrimSpace(raw)
	lower := strings.ToLower(value)
	switch {
	case strings.HasPrefix(lower, "精准:"):
		return strings.TrimSpace(value[len("精准:"):]), "exact"
	case strings.HasPrefix(lower, "模糊:"):
		return strings.TrimSpace(value[len("模糊:"):]), "fuzzy"
	case strings.HasPrefix(lower, "[精准]"):
		return strings.TrimSpace(value[len("[精准]"):]), "exact"
	case strings.HasPrefix(lower, "[模糊]"):
		return strings.TrimSpace(value[len("[模糊]"):]), "fuzzy"
	default:
		return strings.TrimSpace(value), ""
	}
}

func shouldDeleteKeywordRule(rule model.KeywordRule, selectors []keywordDeleteSelector) bool {
	if len(selectors) == 0 {
		return false
	}
	ruleText := strings.ToLower(strings.TrimSpace(rule.Text))
	ruleMode := normalizeKeywordMode(rule.Mode)
	for _, selector := range selectors {
		if selector.text != ruleText {
			continue
		}
		if selector.mode == "" || normalizeKeywordMode(selector.mode) == ruleMode {
			return true
		}
	}
	return false
}

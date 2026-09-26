package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type KeywordStore struct {
	path string
	mu   sync.RWMutex
	data keywordFile
}

type keywordFile struct {
	Keywords []string `json:"keywords"`
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
	return append([]string(nil), s.data.Keywords...), nil
}

func (s *KeywordStore) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.data.Keywords...)
}

func (s *KeywordStore) Add(raw []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := make(map[string]struct{}, len(s.data.Keywords))
	for _, keyword := range s.data.Keywords {
		existing[strings.ToLower(strings.TrimSpace(keyword))] = struct{}{}
	}

	added := 0
	for _, keyword := range raw {
		keyword = strings.TrimSpace(keyword)
		if keyword == "" {
			continue
		}
		lower := strings.ToLower(keyword)
		if _, ok := existing[lower]; ok {
			continue
		}
		s.data.Keywords = append(s.data.Keywords, keyword)
		existing[lower] = struct{}{}
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

	toDelete := make(map[string]struct{}, len(raw))
	for _, keyword := range raw {
		keyword = strings.ToLower(strings.TrimSpace(keyword))
		if keyword != "" {
			toDelete[keyword] = struct{}{}
		}
	}

	filtered := make([]string, 0, len(s.data.Keywords))
	removed := 0
	for _, keyword := range s.data.Keywords {
		if _, ok := toDelete[strings.ToLower(strings.TrimSpace(keyword))]; ok {
			removed++
			continue
		}
		filtered = append(filtered, keyword)
	}
	s.data.Keywords = filtered
	if removed == 0 {
		return 0, nil
	}
	return removed, s.persistLocked()
}

func (s *KeywordStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o644)
}

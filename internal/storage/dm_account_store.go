package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode"
)

type DMAccount struct {
	Phone       string `json:"phone"`
	SessionFile string `json:"session_file"`
}

type DMAccountStore struct {
	path        string
	sessionsDir string

	mu       sync.RWMutex
	accounts []DMAccount
}

func NewDMAccountStore(path, sessionsDir string) (*DMAccountStore, error) {
	store := &DMAccountStore{
		path:        path,
		sessionsDir: sessionsDir,
		accounts:    make([]DMAccount, 0),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *DMAccountStore) List() []DMAccount {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]DMAccount, len(s.accounts))
	copy(result, s.accounts)
	return result
}

func (s *DMAccountStore) Get(phone string) (DMAccount, bool) {
	normalized := normalizePhone(phone)
	if normalized == "" {
		return DMAccount{}, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, account := range s.accounts {
		if account.Phone == normalized {
			return account, true
		}
	}
	return DMAccount{}, false
}

func (s *DMAccountStore) Add(phone string) (DMAccount, bool, error) {
	normalized := normalizePhone(phone)
	if normalized == "" {
		return DMAccount{}, false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, account := range s.accounts {
		if account.Phone == normalized {
			return account, false, nil
		}
	}

	account := DMAccount{
		Phone:       normalized,
		SessionFile: filepath.Join(s.sessionsDir, sanitizeDMPhone(normalized)+".json"),
	}
	s.accounts = append(s.accounts, account)
	sort.Slice(s.accounts, func(i, j int) bool {
		return s.accounts[i].Phone < s.accounts[j].Phone
	})
	if err := s.persistLocked(); err != nil {
		return DMAccount{}, false, err
	}
	return account, true, nil
}

func (s *DMAccountStore) Remove(phone string) (DMAccount, bool, error) {
	normalized := normalizePhone(phone)
	if normalized == "" {
		return DMAccount{}, false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, account := range s.accounts {
		if account.Phone != normalized {
			continue
		}

		s.accounts = append(s.accounts[:i], s.accounts[i+1:]...)
		if err := s.persistLocked(); err != nil {
			return DMAccount{}, false, err
		}
		return account, true, nil
	}
	return DMAccount{}, false, nil
}

func (s *DMAccountStore) load() error {
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

	if err := json.Unmarshal(data, &s.accounts); err != nil {
		return err
	}
	for i := range s.accounts {
		s.accounts[i].Phone = normalizePhone(s.accounts[i].Phone)
		if strings.TrimSpace(s.accounts[i].SessionFile) == "" {
			s.accounts[i].SessionFile = filepath.Join(s.sessionsDir, sanitizeDMPhone(s.accounts[i].Phone)+".json")
		}
	}
	sort.Slice(s.accounts, func(i, j int) bool {
		return s.accounts[i].Phone < s.accounts[j].Phone
	})
	return nil
}

func (s *DMAccountStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(s.sessionsDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.accounts, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o644)
}

func sanitizeDMPhone(phone string) string {
	var b strings.Builder
	for _, r := range phone {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "dm"
	}
	return "dm_" + b.String()
}

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

type MonitorAccount struct {
	Phone       string `json:"phone"`
	SessionFile string `json:"session_file"`
}

type MonitorAccountStore struct {
	path        string
	sessionsDir string

	mu       sync.RWMutex
	accounts []MonitorAccount
}

func NewMonitorAccountStore(path, sessionsDir string) (*MonitorAccountStore, error) {
	store := &MonitorAccountStore{
		path:        path,
		sessionsDir: sessionsDir,
		accounts:    make([]MonitorAccount, 0),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *MonitorAccountStore) List() []MonitorAccount {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]MonitorAccount, len(s.accounts))
	copy(result, s.accounts)
	return result
}

func (s *MonitorAccountStore) Get(phone string) (MonitorAccount, bool) {
	normalized := normalizePhone(phone)
	if normalized == "" {
		return MonitorAccount{}, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, account := range s.accounts {
		if account.Phone == normalized {
			return account, true
		}
	}
	return MonitorAccount{}, false
}

func (s *MonitorAccountStore) Add(phone string) (MonitorAccount, bool, error) {
	normalized := normalizePhone(phone)
	if normalized == "" {
		return MonitorAccount{}, false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, account := range s.accounts {
		if account.Phone == normalized {
			return account, false, nil
		}
	}

	account := MonitorAccount{
		Phone:       normalized,
		SessionFile: filepath.Join(s.sessionsDir, sanitizePhone(normalized)+".json"),
	}
	s.accounts = append(s.accounts, account)
	sort.Slice(s.accounts, func(i, j int) bool {
		return s.accounts[i].Phone < s.accounts[j].Phone
	})
	if err := s.persistLocked(); err != nil {
		return MonitorAccount{}, false, err
	}
	return account, true, nil
}

func (s *MonitorAccountStore) Remove(phone string) (MonitorAccount, bool, error) {
	normalized := normalizePhone(phone)
	if normalized == "" {
		return MonitorAccount{}, false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i, account := range s.accounts {
		if account.Phone != normalized {
			continue
		}

		s.accounts = append(s.accounts[:i], s.accounts[i+1:]...)
		if err := s.persistLocked(); err != nil {
			return MonitorAccount{}, false, err
		}
		return account, true, nil
	}
	return MonitorAccount{}, false, nil
}

func (s *MonitorAccountStore) load() error {
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
			s.accounts[i].SessionFile = filepath.Join(s.sessionsDir, sanitizePhone(s.accounts[i].Phone)+".json")
		}
	}
	sort.Slice(s.accounts, func(i, j int) bool {
		return s.accounts[i].Phone < s.accounts[j].Phone
	})
	return nil
}

func (s *MonitorAccountStore) persistLocked() error {
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

func normalizePhone(phone string) string {
	phone = strings.TrimSpace(phone)
	phone = strings.ReplaceAll(phone, " ", "")
	return phone
}

func sanitizePhone(phone string) string {
	var b strings.Builder
	for _, r := range phone {
		if unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "monitor"
	}
	return "monitor_" + b.String()
}

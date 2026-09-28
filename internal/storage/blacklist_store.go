package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type BlacklistStore struct {
	path string

	mu    sync.RWMutex
	state blacklistState
}

type blacklistState struct {
	Users []BlockedUser `json:"users"`
	Chats []BlockedChat `json:"chats"`
}

type BlockedUser struct {
	UserID    int64     `json:"user_id"`
	Username  string    `json:"username"`
	BlockedAt time.Time `json:"blocked_at"`
}

type BlockedChat struct {
	ChatID    int64     `json:"chat_id"`
	Title     string    `json:"title"`
	BlockedAt time.Time `json:"blocked_at"`
}

func NewBlacklistStore(path string) (*BlacklistStore, error) {
	store := &BlacklistStore{
		path: path,
		state: blacklistState{
			Users: make([]BlockedUser, 0),
			Chats: make([]BlockedChat, 0),
		},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *BlacklistStore) IsUserBlocked(userID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, user := range s.state.Users {
		if user.UserID == userID {
			return true
		}
	}
	return false
}

func (s *BlacklistStore) IsChatBlocked(chatID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, chat := range s.state.Chats {
		if chat.ChatID == chatID {
			return true
		}
	}
	return false
}

func (s *BlacklistStore) AddUser(userID int64, username string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, user := range s.state.Users {
		if user.UserID == userID {
			return false, nil
		}
	}
	s.state.Users = append(s.state.Users, BlockedUser{
		UserID:    userID,
		Username:  username,
		BlockedAt: time.Now(),
	})
	sort.Slice(s.state.Users, func(i, j int) bool { return s.state.Users[i].UserID < s.state.Users[j].UserID })
	return true, s.persistLocked()
}

func (s *BlacklistStore) AddChat(chatID int64, title string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, chat := range s.state.Chats {
		if chat.ChatID == chatID {
			return false, nil
		}
	}
	s.state.Chats = append(s.state.Chats, BlockedChat{
		ChatID:    chatID,
		Title:     title,
		BlockedAt: time.Now(),
	})
	sort.Slice(s.state.Chats, func(i, j int) bool { return s.state.Chats[i].ChatID < s.state.Chats[j].ChatID })
	return true, s.persistLocked()
}

func (s *BlacklistStore) RemoveUser(userID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	filtered := s.state.Users[:0]
	removed := false
	for _, user := range s.state.Users {
		if user.UserID == userID {
			removed = true
			continue
		}
		filtered = append(filtered, user)
	}
	if !removed {
		return false, nil
	}
	s.state.Users = filtered
	return true, s.persistLocked()
}

func (s *BlacklistStore) RemoveChat(chatID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	filtered := s.state.Chats[:0]
	removed := false
	for _, chat := range s.state.Chats {
		if chat.ChatID == chatID {
			removed = true
			continue
		}
		filtered = append(filtered, chat)
	}
	if !removed {
		return false, nil
	}
	s.state.Chats = filtered
	return true, s.persistLocked()
}

func (s *BlacklistStore) Users() []BlockedUser {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]BlockedUser, len(s.state.Users))
	copy(result, s.state.Users)
	return result
}

func (s *BlacklistStore) Chats() []BlockedChat {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]BlockedChat, len(s.state.Chats))
	copy(result, s.state.Chats)
	return result
}

func (s *BlacklistStore) load() error {
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
	return json.Unmarshal(data, &s.state)
}

func (s *BlacklistStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o644)
}

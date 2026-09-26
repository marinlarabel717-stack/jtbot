package storage

import (
	"encoding/json"
	"fmt"
	"os"
)

type KeywordStore struct {
	path string
}

type keywordFile struct {
	Keywords []string `json:"keywords"`
}

func NewKeywordStore(path string) *KeywordStore {
	return &KeywordStore{path: path}
}

func (s *KeywordStore) Load() ([]string, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("read keywords file: %w", err)
	}

	var file keywordFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("parse keywords file: %w", err)
	}
	return file.Keywords, nil
}

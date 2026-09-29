package matcher

import (
	"strings"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type KeywordMatcher struct{}

func NewKeywordMatcher() *KeywordMatcher {
	return &KeywordMatcher{}
}

func (m *KeywordMatcher) Match(text string, keywords []model.KeywordRule) []string {
	text = strings.TrimSpace(text)
	textLower := strings.ToLower(text)
	matched := make([]string, 0)
	seen := make(map[string]struct{})
	for _, keyword := range keywords {
		value := strings.TrimSpace(keyword.Text)
		if value == "" {
			continue
		}
		keywordLower := strings.ToLower(value)
		if _, exists := seen[keywordLower]; exists {
			continue
		}
		switch normalizeMode(keyword.Mode) {
		case "exact":
			if strings.EqualFold(text, value) {
				matched = append(matched, value)
				seen[keywordLower] = struct{}{}
			}
		default:
			if strings.Contains(textLower, keywordLower) {
				matched = append(matched, value)
				seen[keywordLower] = struct{}{}
			}
		}
	}
	return matched
}

func normalizeMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "exact":
		return "exact"
	default:
		return "fuzzy"
	}
}

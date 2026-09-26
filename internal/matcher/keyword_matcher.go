package matcher

import "strings"

type KeywordMatcher struct{}

func NewKeywordMatcher() *KeywordMatcher {
	return &KeywordMatcher{}
}

func (m *KeywordMatcher) Match(text string, keywords []string) []string {
	textLower := strings.ToLower(text)
	matched := make([]string, 0)
	seen := make(map[string]struct{})
	for _, keyword := range keywords {
		keyword = strings.TrimSpace(keyword)
		if keyword == "" {
			continue
		}
		keywordLower := strings.ToLower(keyword)
		if _, exists := seen[keywordLower]; exists {
			continue
		}
		if strings.Contains(textLower, keywordLower) {
			matched = append(matched, keyword)
			seen[keywordLower] = struct{}{}
		}
	}
	return matched
}

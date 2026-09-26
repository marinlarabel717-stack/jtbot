package matcher

import "strings"

type KeywordMatcher struct {
	keywords []string
}

func NewKeywordMatcher(keywords []string) *KeywordMatcher {
	cleaned := make([]string, 0, len(keywords))
	for _, keyword := range keywords {
		keyword = strings.TrimSpace(keyword)
		if keyword == "" {
			continue
		}
		cleaned = append(cleaned, keyword)
	}
	return &KeywordMatcher{keywords: cleaned}
}

func (m *KeywordMatcher) Match(text string) []string {
	textLower := strings.ToLower(text)
	matched := make([]string, 0)
	seen := make(map[string]struct{})
	for _, keyword := range m.keywords {
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

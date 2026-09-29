package matcher

import (
	"reflect"
	"testing"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

func TestKeywordMatcherSupportsFuzzyAndExact(t *testing.T) {
	t.Parallel()

	m := NewKeywordMatcher()
	rules := []model.KeywordRule{
		{Text: "合作", Mode: "fuzzy"},
		{Text: "TG", Mode: "exact"},
	}

	if got := m.Match("我想合作，欢迎私信", rules); !reflect.DeepEqual(got, []string{"合作"}) {
		t.Fatalf("unexpected fuzzy match: %#v", got)
	}
	if got := m.Match("TG", rules); !reflect.DeepEqual(got, []string{"TG"}) {
		t.Fatalf("unexpected exact match: %#v", got)
	}
	if got := m.Match("TG项目", rules); len(got) != 0 {
		t.Fatalf("expected exact keyword to reject partial match, got %#v", got)
	}
}

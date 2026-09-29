package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKeywordStoreSupportsModesAndLegacyLoad(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "keywords.json")
	if err := os.WriteFile(path, []byte(`{"keywords":["合作"]}`), 0o644); err != nil {
		t.Fatalf("write keywords file: %v", err)
	}

	store := NewKeywordStore(path)
	if _, err := store.Load(); err != nil {
		t.Fatalf("load keywords: %v", err)
	}
	if added, err := store.AddWithMode("exact", []string{"TG"}); err != nil || added != 1 {
		t.Fatalf("add exact keyword: added=%d err=%v", added, err)
	}
	if removed, err := store.Remove([]string{"精准:TG"}); err != nil || removed != 1 {
		t.Fatalf("remove exact keyword: removed=%d err=%v", removed, err)
	}

	entries := store.ListEntries()
	if len(entries) != 1 || entries[0].Text != "合作" || entries[0].Mode != "fuzzy" {
		t.Fatalf("unexpected entries after remove: %#v", entries)
	}
}

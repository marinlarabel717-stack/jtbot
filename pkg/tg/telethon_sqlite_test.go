package tg

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	gsession "github.com/gotd/td/session"
	_ "modernc.org/sqlite"
)

func TestConvertTelethonSQLiteSessionFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "sample.session")
	targetPath := filepath.Join(tmpDir, "sample.json")

	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
		CREATE TABLE sessions (
			dc_id INTEGER PRIMARY KEY,
			server_address TEXT,
			port INTEGER,
			auth_key BLOB,
			takeout_id INTEGER
		)
	`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	authKey := bytes.Repeat([]byte{0x42}, 256)
	if _, err := db.Exec(
		`INSERT INTO sessions (dc_id, server_address, port, auth_key, takeout_id) VALUES (?, ?, ?, ?, NULL)`,
		2,
		"149.154.167.50",
		443,
		authKey,
	); err != nil {
		t.Fatalf("insert session row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	if err := ConvertTelethonSQLiteSessionFile(context.Background(), sourcePath, targetPath); err != nil {
		t.Fatalf("convert telethon session: %v", err)
	}

	loader := gsession.Loader{
		Storage: &gsession.FileStorage{Path: targetPath},
	}
	data, err := loader.Load(context.Background())
	if err != nil {
		t.Fatalf("load gotd session: %v", err)
	}

	if data.DC != 2 {
		t.Fatalf("expected dc 2, got %d", data.DC)
	}
	if data.Addr != "149.154.167.50:443" {
		t.Fatalf("unexpected addr: %s", data.Addr)
	}
	if !bytes.Equal(data.AuthKey, authKey) {
		t.Fatalf("auth key mismatch")
	}
	if len(data.AuthKeyID) == 0 {
		t.Fatalf("expected auth key id")
	}
}

func TestExtractTelethonSessionFilesFromZip(t *testing.T) {
	t.Parallel()

	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)

	addZipFile := func(name string, data []byte) {
		t.Helper()
		part, err := writer.Create(name)
		if err != nil {
			t.Fatalf("create zip file: %v", err)
		}
		if _, err := part.Write(data); err != nil {
			t.Fatalf("write zip file: %v", err)
		}
	}

	addZipFile("a.session", []byte("one"))
	addZipFile("nested/b.session", []byte("two"))
	addZipFile("nested/b.session-journal", []byte("skip"))
	addZipFile("note.txt", []byte("ignore"))

	if err := writer.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}

	files, err := ExtractTelethonSessionFiles("batch.zip", archive.Bytes())
	if err != nil {
		t.Fatalf("extract session files: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 session files, got %d", len(files))
	}
	if files[0].Name != "a.session" || files[1].Name != "b.session" {
		t.Fatalf("unexpected file names: %+v", files)
	}
}

func TestSaveTelethonSQLiteSession(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "sample.session")
	targetPath := filepath.Join(tmpDir, "saved.json")

	db, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE sessions (
			dc_id INTEGER PRIMARY KEY,
			server_address TEXT,
			port INTEGER,
			auth_key BLOB,
			takeout_id INTEGER
		)
	`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (dc_id, server_address, port, auth_key, takeout_id) VALUES (?, ?, ?, ?, NULL)`,
		4,
		"149.154.167.91",
		443,
		bytes.Repeat([]byte{0x24}, 256),
	); err != nil {
		t.Fatalf("insert session row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	raw, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read sqlite session: %v", err)
	}
	if err := SaveTelethonSQLiteSession(context.Background(), raw, targetPath); err != nil {
		t.Fatalf("save telethon sqlite session: %v", err)
	}
	if _, err := os.Stat(targetPath); err != nil {
		t.Fatalf("stat saved session: %v", err)
	}
}

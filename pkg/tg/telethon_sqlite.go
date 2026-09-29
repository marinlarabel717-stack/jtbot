package tg

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gotd/td/crypto"
	gsession "github.com/gotd/td/session"
	_ "modernc.org/sqlite"
)

type ImportedSessionFile struct {
	Name string
	Data []byte
}

func ExtractTelethonSessionFiles(filename string, data []byte) ([]ImportedSessionFile, error) {
	name := strings.ToLower(strings.TrimSpace(filename))
	switch {
	case strings.HasSuffix(name, ".session"):
		base := filepath.Base(filename)
		if strings.TrimSpace(base) == "" {
			base = "imported.session"
		}
		return []ImportedSessionFile{{
			Name: base,
			Data: append([]byte(nil), data...),
		}}, nil
	case strings.HasSuffix(name, ".zip"):
		reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("open zip: %w", err)
		}

		files := make([]ImportedSessionFile, 0)
		for _, file := range reader.File {
			if file.FileInfo().IsDir() {
				continue
			}

			base := filepath.Base(file.Name)
			lowerBase := strings.ToLower(base)
			if base == "" || strings.HasSuffix(lowerBase, ".session-journal") || !strings.HasSuffix(lowerBase, ".session") {
				continue
			}

			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("open zip item %s: %w", file.Name, err)
			}

			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(rc); err != nil {
				_ = rc.Close()
				return nil, fmt.Errorf("read zip item %s: %w", file.Name, err)
			}
			_ = rc.Close()

			files = append(files, ImportedSessionFile{
				Name: base,
				Data: buf.Bytes(),
			})
		}

		sort.Slice(files, func(i, j int) bool {
			return files[i].Name < files[j].Name
		})
		if len(files) == 0 {
			return nil, fmt.Errorf("zip 中没有 .session 文件")
		}
		return files, nil
	default:
		return nil, fmt.Errorf("仅支持 .session 或 .zip 文件")
	}
}

func SaveTelethonSQLiteSession(ctx context.Context, sqliteData []byte, targetPath string) error {
	tmpFile, err := os.CreateTemp("", "jtbot-telethon-*.session")
	if err != nil {
		return fmt.Errorf("create temp session: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(sqliteData); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("write temp session: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp session: %w", err)
	}

	return ConvertTelethonSQLiteSessionFile(ctx, tmpPath, targetPath)
}

func ConvertTelethonSQLiteSessionFile(ctx context.Context, sourcePath, targetPath string) error {
	dsn := fmt.Sprintf("file:%s?mode=ro", filepath.ToSlash(sourcePath))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open sqlite session: %w", err)
	}
	defer db.Close()

	var (
		dcID          int
		serverAddress string
		port          int
		authKey       []byte
	)

	row := db.QueryRowContext(ctx, `
		SELECT dc_id, server_address, port, auth_key
		FROM sessions
		WHERE auth_key IS NOT NULL AND length(auth_key) > 0
		ORDER BY dc_id
		LIMIT 1
	`)
	if err := row.Scan(&dcID, &serverAddress, &port, &authKey); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("session 未授权或缺少 auth_key")
		}
		return fmt.Errorf("query telethon session: %w", err)
	}

	serverAddress = strings.TrimSpace(serverAddress)
	if dcID <= 0 {
		return fmt.Errorf("session dc_id 无效: %d", dcID)
	}
	if serverAddress == "" {
		return fmt.Errorf("session server_address 为空")
	}
	if port <= 0 {
		return fmt.Errorf("session port 无效: %d", port)
	}
	if len(authKey) == 0 {
		return fmt.Errorf("session auth_key 为空")
	}

	var key crypto.Key
	copy(key[:], authKey)
	keyID := key.WithID().ID

	data := &gsession.Data{
		DC:        dcID,
		Addr:      net.JoinHostPort(serverAddress, strconv.Itoa(port)),
		AuthKey:   append([]byte(nil), authKey...),
		AuthKeyID: keyID[:],
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create session dir: %w", err)
	}

	loader := gsession.Loader{
		Storage: &gsession.FileStorage{Path: targetPath},
	}
	if err := loader.Save(ctx, data); err != nil {
		return fmt.Errorf("save gotd session: %w", err)
	}
	return nil
}

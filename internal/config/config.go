package config

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppID          int
	AppHash        string
	Phone          string
	SessionFile    string
	MonitorChatIDs map[int64]struct{}
	AlertChatID    int64
	KeywordsFile   string
	RecordsFile    string
	SettingsFile   string
	DMTemplate     string
	Cooldown       time.Duration
	QueueSize      int
	PollTimeout    int
	DryRun         bool
	LogLevel       string
}

func LoadDotEnv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}

	return scanner.Err()
}

func Load() (Config, error) {
	recordsFile := getEnv("RECORDS_FILE", filepath.Join("data", "records.json"))
	keywordsFile := getEnv("KEYWORDS_FILE", filepath.Join("configs", "keywords.example.json"))
	settingsFile := getEnv("SETTINGS_FILE", filepath.Join("data", "settings.json"))
	sessionFile := getEnv("SESSION_FILE", filepath.Join("data", "session.json"))
	cooldownMinutes := getEnvInt("COOLDOWN_MINUTES", 1440)

	cfg := Config{
		AppID:          getEnvInt("APP_ID", 0),
		AppHash:        strings.TrimSpace(os.Getenv("APP_HASH")),
		Phone:          strings.TrimSpace(os.Getenv("PHONE")),
		SessionFile:    sessionFile,
		MonitorChatIDs: parseChatIDs(getEnv("MONITOR_CHAT_IDS", os.Getenv("MONITOR_CHAT_ID"))),
		AlertChatID:    getEnvInt64("ALERT_CHAT_ID", 0),
		KeywordsFile:   keywordsFile,
		RecordsFile:    recordsFile,
		SettingsFile:   settingsFile,
		DMTemplate:     getEnv("DM_TEMPLATE", "你好，看到你刚刚在 {chat_title} 提到 {keywords}，如果你愿意的话可以继续聊聊。"),
		Cooldown:       time.Duration(cooldownMinutes) * time.Minute,
		QueueSize:      getEnvInt("QUEUE_SIZE", 100),
		PollTimeout:    getEnvInt("POLL_TIMEOUT", 30),
		DryRun:         getEnvBool("DRY_RUN", false),
		LogLevel:       strings.ToLower(getEnv("LOG_LEVEL", "info")),
	}

	if cfg.AppID == 0 {
		return Config{}, errors.New("APP_ID is required")
	}
	if cfg.AppHash == "" {
		return Config{}, errors.New("APP_HASH is required")
	}
	if cfg.Phone == "" {
		return Config{}, errors.New("PHONE is required")
	}
	if cfg.QueueSize < 1 {
		cfg.QueueSize = 100
	}
	if cfg.PollTimeout < 1 {
		cfg.PollTimeout = 30
	}

	return cfg, nil
}

func parseChatIDs(raw string) map[int64]struct{} {
	result := make(map[int64]struct{})
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			continue
		}
		result[id] = struct{}{}
	}
	return result
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func getEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvInt64(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

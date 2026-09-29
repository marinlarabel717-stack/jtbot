package sender

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/internal/storage"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type DMDispatcher interface {
	SendDM(ctx context.Context, fallback *tg.Client, job model.DMJob, text string) (string, error)
}

type Sender struct {
	client          *tg.Client
	dispatcher      DMDispatcher
	recordStore     *storage.RecordStore
	settings        *storage.SettingsStore
	logger          *logx.Logger
	notifyClient    *tg.Client
	notifyAdminChat int64
}

func New(client *tg.Client, dispatcher DMDispatcher, recordStore *storage.RecordStore, settings *storage.SettingsStore, logger *logx.Logger, notifyClient *tg.Client, notifyAdminChat int64) *Sender {
	return &Sender{
		client:          client,
		dispatcher:      dispatcher,
		recordStore:     recordStore,
		settings:        settings,
		logger:          logger,
		notifyClient:    notifyClient,
		notifyAdminChat: notifyAdminChat,
	}
}

func (s *Sender) Run(ctx context.Context, jobs <-chan model.DMJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			s.handleJob(ctx, job)
		}
	}
}

func (s *Sender) handleJob(ctx context.Context, job model.DMJob) {
	text := s.renderTemplate(job)
	record := model.DMRecord{
		UserID:     job.TargetUserID,
		Username:   job.Username,
		ChatID:     job.ChatID,
		Keywords:   append([]string(nil), job.Keywords...),
		SourceText: job.SourceText,
		Message:    text,
		SentAt:     time.Now(),
	}

	if s.settings.IsDryRun() {
		record.Sender = "dry_run"
		record.Status = "dry_run"
		s.recordStore.SaveDMRecord(record)
		s.logger.Infof("dry-run dm user=%d keywords=%s", job.TargetUserID, strings.Join(job.Keywords, ","))
		return
	}

	senderLabel := "telegram"
	if s.client != nil {
		senderLabel = s.client.Label()
	}
	var err error
	if s.dispatcher != nil {
		senderLabel, err = s.dispatcher.SendDM(ctx, s.client, job, text)
	} else {
		err = s.client.SendDirectMessage(ctx, job.TargetUserID, job.Username, text)
	}
	record.Sender = senderLabel

	if err != nil {
		record.Status = "failed"
		record.Error = translateDMError(err)
		s.recordStore.SaveDMRecord(record)
		s.notifyDMStatus(ctx, record, job)
		s.logger.Errorf("send dm failed user=%d err=%v", job.TargetUserID, err)
		return
	}

	record.Status = "sent"
	s.recordStore.SaveDMRecord(record)
	s.notifyDMStatus(ctx, record, job)
	s.logger.Infof("dm sent user=%d keywords=%s", job.TargetUserID, strings.Join(job.Keywords, ","))
}

func (s *Sender) renderTemplate(job model.DMJob) string {
	replacer := strings.NewReplacer(
		"{username}", safeValue(job.Username, "friend"),
		"{chat_title}", safeValue(job.ChatTitle, "group"),
		"{keywords}", strings.Join(job.Keywords, ", "),
		"{message}", job.SourceText,
	)
	return replacer.Replace(s.settings.DMTemplate())
}

func safeValue(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func (s *Sender) notifyDMStatus(ctx context.Context, record model.DMRecord, job model.DMJob) {
	if s.notifyAdminChat == 0 {
		return
	}

	client := s.notifyClient
	if client == nil {
		client = s.client
	}
	if client == nil {
		return
	}

	text := formatDMStatusText(record, job)
	if err := client.SendMessage(ctx, s.notifyAdminChat, text, nil); err != nil {
		s.logger.Errorf("send dm status notification failed: %v", err)
	}
}

func formatDMStatusText(record model.DMRecord, job model.DMJob) string {
	title := "✅ 私信发送成功"
	statusLine := fmt.Sprintf("📱 发送账号: %s", safeValue(record.Sender, "未分配"))
	if record.Status != "sent" {
		title = "❌ 私信发送失败"
		statusLine = fmt.Sprintf("%s\n⚠️ 失败原因: %s", statusLine, safeValue(record.Error, "发送失败"))
	}

	lines := []string{
		title,
		"",
		fmt.Sprintf("👤 目标用户: %s", formatDMTargetLabel(job, record)),
		fmt.Sprintf("⏰ 发送时间: %s", record.SentAt.Format("2006-01-02 15:04:05")),
		fmt.Sprintf("📍 来源群组: %s", safeValue(job.ChatTitle, "unknown")),
		fmt.Sprintf("🔗 群组链接: %s", safeValue(job.ChatLink, "暂无公开链接")),
		statusLine,
	}
	return strings.Join(lines, "\n")
}

func formatDMTargetLabel(job model.DMJob, record model.DMRecord) string {
	if label := strings.TrimSpace(job.TargetLabel); label != "" {
		return fmt.Sprintf("%s (%d)", label, record.UserID)
	}
	if username := strings.TrimSpace(record.Username); username != "" {
		return fmt.Sprintf("@%s (%d)", strings.TrimPrefix(username, "@"), record.UserID)
	}
	return fmt.Sprintf("%d", record.UserID)
}

func translateDMError(err error) string {
	if err == nil {
		return ""
	}

	message := strings.TrimSpace(err.Error())
	lower := strings.ToLower(message)

	switch {
	case strings.Contains(lower, "deadline exceeded"):
		return "请求超时了，私信这次没有发出去"
	case strings.Contains(lower, "no usable dm sending account"),
		strings.Contains(message, "没有可用的私信发送账号"):
		return "没有可用的私信号，请先在私信号池添加或上传私信号"
	case strings.Contains(lower, "peer not cached yet"):
		return "目标用户当前还没有被这个账号识别到，暂时无法直接发私信"
	case strings.Contains(lower, "telegram api is not ready"):
		return "私信号还没完全上线，暂时不能发送私信"
	case strings.Contains(lower, "username is empty"):
		return "目标用户没有可用用户名，当前账号也没有缓存到这个用户"
	case strings.Contains(lower, "privacy"):
		return "对方开启了隐私限制，当前账号不能给他发私信"
	case strings.Contains(lower, "peer_flood"):
		return "该账号触发了 Telegram 私信风控，暂时不能继续发"
	case strings.Contains(lower, "flood_wait"):
		return "该账号触发了发送频率限制，需要稍后再试"
	case strings.Contains(lower, "user_is_bot"):
		return "目标是机器人账号，不能发送私信"
	case strings.Contains(lower, "input user deactivated"),
		strings.Contains(lower, "user_deactivated"):
		return "目标用户账号已注销或不可用"
	case strings.Contains(lower, "forbidden"),
		strings.Contains(lower, "chat_write_forbidden"):
		return "当前账号没有权限给这个目标发送消息"
	default:
		return message
	}
}

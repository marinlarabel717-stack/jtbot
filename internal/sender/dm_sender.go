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
	SendDM(ctx context.Context, fallback *tg.Client, job model.DMJob, payload model.DMTemplatePayload) (string, error)
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
	payload := s.renderTemplate(job)
	record := model.DMRecord{
		UserID:     job.TargetUserID,
		Username:   job.Username,
		ChatID:     job.ChatID,
		Keywords:   append([]string(nil), job.Keywords...),
		SourceText: job.SourceText,
		Message:    payload.Summary(),
		SentAt:     time.Now(),
	}

	if s.settings.IsDryRun() {
		record.Sender = "dry_run"
		record.Status = "dry_run"
		s.recordStore.SaveDMRecord(record)
		s.logger.Infof("演练模式：用户=%d，关键词=%s，本次不真实私信", job.TargetUserID, strings.Join(job.Keywords, ","))
		return
	}

	senderLabel := "telegram"
	if s.client != nil {
		senderLabel = s.client.Label()
	}
	var err error
	if s.dispatcher != nil {
		senderLabel, err = s.dispatcher.SendDM(ctx, s.client, job, payload)
	} else {
		err = s.client.SendDirectMessage(ctx, job.TargetUserID, job.Username, payload.Message)
	}
	record.Sender = senderLabel

	if err != nil {
		record.Status = "failed"
		record.Error = TranslateDMError(err)
		s.recordStore.SaveDMRecord(record)
		s.notifyDMStatus(ctx, record, job)
		s.logger.Errorf("私信发送失败：用户=%d，原因=%s", job.TargetUserID, record.Error)
		return
	}

	record.Status = "sent"
	s.recordStore.SaveDMRecord(record)
	s.notifyDMStatus(ctx, record, job)
	s.logger.Infof("私信发送成功：用户=%d，关键词=%s，发送账号=%s", job.TargetUserID, strings.Join(job.Keywords, ","), senderLabel)
}

func (s *Sender) renderTemplate(job model.DMJob) model.DMTemplatePayload {
	raw := strings.TrimSpace(s.settings.RandomDMTemplate())
	if raw == "" {
		raw = model.EncodeTextDMTemplate(s.settings.DMTemplate())
	}
	return model.ParseDMTemplate(raw).Render(job)
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
		s.logger.Errorf("发送私信状态通知失败：%v", err)
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

func TranslateDMError(err error) string {
	if err == nil {
		return ""
	}

	message := strings.TrimSpace(err.Error())
	lower := strings.ToLower(message)

	switch {
	case strings.Contains(lower, "deadline exceeded"):
		return "请求超时了，这次私信没有真正发出去"
	case strings.Contains(lower, "no usable dm sending account"),
		strings.Contains(lower, "没有可用的私信发送账号"):
		return "没有可用的私信号，请先去私信号池补充账号"
	case strings.Contains(lower, "username_not_occupied"):
		return "目标用户名已经不存在了，通常是对方改名、注销，或者你拿到的是旧用户名；当前账号也没能通过用户 ID 直接定位到他，所以这次发不出去"
	case strings.Contains(lower, "peer not cached yet"):
		return "这个私信号还没识别过目标用户，当前没有和对方建立可发送的会话，所以暂时发不出去"
	case strings.Contains(lower, "telegram api is not ready"):
		return "私信号还没完全上线，暂时不能发送私信"
	case strings.Contains(lower, "username is empty"):
		return "目标用户没有可用用户名，而且当前私信号也没缓存到这个人，所以发不出去"
	case strings.Contains(lower, "peer_id_invalid"):
		return "目标会话无效，通常是这个私信号还没真正拿到对方会话，或者目标资料已经变了"
	case strings.Contains(lower, "privacy"):
		return "对方开启了隐私限制，当前账号不能主动给他发私信"
	case strings.Contains(lower, "peer_flood"):
		return "这个私信号触发了 Telegram 私信风控，暂时不能继续发人"
	case strings.Contains(lower, "flood_wait"):
		return "这个私信号触发了发送频率限制，需要等一会儿再试"
	case strings.Contains(lower, "user_is_bot"):
		return "目标是机器人账号，不能给机器人发这种私信"
	case strings.Contains(lower, "input user deactivated"),
		strings.Contains(lower, "user_deactivated"):
		return "目标账号已经注销或失效，不能再私信"
	case strings.Contains(lower, "forbidden"),
		strings.Contains(lower, "chat_write_forbidden"):
		return "当前私信号没有权限给这个目标发送消息"
	case strings.Contains(lower, "rpc error code 400"):
		return "Telegram 拒绝了这次发送请求，通常是目标用户资料变了、用户名失效，或者当前账号拿不到可发送会话"
	default:
		return message
	}
}

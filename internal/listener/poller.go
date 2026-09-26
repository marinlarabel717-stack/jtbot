package listener

import (
	"context"
	"time"

	"github.com/marinlarabel717-stack/jtbot/internal/logx"
	"github.com/marinlarabel717-stack/jtbot/internal/model"
	"github.com/marinlarabel717-stack/jtbot/pkg/tg"
)

type Handler func(context.Context, model.Update) error

type Poller struct {
	client  *tg.Client
	timeout int
	logger  *logx.Logger
	handler Handler
}

func NewPoller(client *tg.Client, timeout int, logger *logx.Logger, handler Handler) *Poller {
	return &Poller{
		client:  client,
		timeout: timeout,
		logger:  logger,
		handler: handler,
	}
}

func (p *Poller) Run(ctx context.Context) error {
	offset := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		updates, err := p.client.GetUpdates(ctx, offset, p.timeout)
		if err != nil {
			p.logger.Errorf("get updates failed: %v", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(3 * time.Second):
			}
			continue
		}

		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			if err := p.handler(ctx, update); err != nil {
				p.logger.Errorf("handle update %d failed: %v", update.UpdateID, err)
			}
		}
	}
}

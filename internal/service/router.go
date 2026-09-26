package service

import (
	"context"

	"github.com/marinlarabel717-stack/jtbot/internal/model"
)

type Router struct {
	admin   *AdminService
	trigger *TriggerService
}

func NewRouter(admin *AdminService, trigger *TriggerService) *Router {
	return &Router{
		admin:   admin,
		trigger: trigger,
	}
}

func (r *Router) HandleUpdate(ctx context.Context, update model.Update) error {
	handled, err := r.admin.HandleUpdate(ctx, update)
	if handled || err != nil {
		return err
	}
	return r.trigger.HandleUpdate(ctx, update)
}

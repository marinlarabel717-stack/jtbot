package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/marinlarabel717-stack/jtbot/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := app.New()
	if err != nil {
		log.Fatalf("init app failed: %v", err)
	}

	if err := application.Run(ctx); err != nil {
		log.Fatalf("run app failed: %v", err)
	}
}

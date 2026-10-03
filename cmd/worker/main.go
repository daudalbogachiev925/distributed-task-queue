package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daudalobogachiev925/distributed-task-queue/internal/config"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/worker"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	coordAddr := config.Getenv("COORDINATOR_ADDR", "localhost:50051")
	workerID := config.Getenv("WORKER_ID", fmt.Sprintf("worker-%d", os.Getpid()))

	handlers := map[string]worker.Handler{
		"send_email": func(ctx context.Context, payload []byte) error {
			log.Printf("sending email: %s", string(payload))
			time.Sleep(500 * time.Millisecond)
			return nil
		},
		"resize_image": func(ctx context.Context, payload []byte) error {
			log.Printf("resizing image: %s", string(payload))
			time.Sleep(1 * time.Second)
			return nil
		},
		"always_fail": func(ctx context.Context, payload []byte) error {
			return fmt.Errorf("boom: always fails")
		},
	}

	w := worker.New(workerID, coordAddr, handlers)
	w.Capabilities = []string{"send_email", "resize_image", "always_fail"}

	log.Printf("worker %s starting, coordinator=%s", workerID, coordAddr)
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker: %v", err)
	}
}

package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/daudalobogachiev925/distributed-task-queue/internal/metrics"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/queue"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/store"
)

type Scheduler struct {
	Q     *queue.Queue
	Store *store.Store
}

func New(q *queue.Queue, s *store.Store) *Scheduler {
	return &Scheduler{Q: q, Store: s}
}

func (s *Scheduler) Run(ctx context.Context) {
	promoteTicker := time.NewTicker(time.Second)
	recoverTicker := time.NewTicker(30 * time.Second)
	metricsTicker := time.NewTicker(5 * time.Second)
	defer promoteTicker.Stop()
	defer recoverTicker.Stop()
	defer metricsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-promoteTicker.C:
			if n, err := s.Q.PromoteDue(ctx); err == nil && n > 0 {
				log.Printf("promoted %d delayed tasks", n)
			}
		case <-recoverTicker.C:
			if n, err := s.Store.RecoverStale(ctx, 2*time.Minute); err == nil && n > 0 {
				log.Printf("recovered %d stale tasks", n)
			}
		case <-metricsTicker.C:
			if d, err := s.Q.Depth(ctx); err == nil {
				metrics.QueueDepth.Set(float64(d))
			}
		}
	}
}

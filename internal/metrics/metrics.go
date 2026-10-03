package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TasksSubmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dtq_tasks_submitted_total",
		Help: "Total number of submitted tasks",
	})
	TasksCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dtq_tasks_completed_total",
	})
	TasksFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dtq_tasks_failed_total",
	}, []string{"task_type"})
	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dtq_task_duration_seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"task_type"})
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dtq_queue_depth",
	})
)

package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"

	pb "github.com/daudalobogachiev925/distributed-task-queue/gen/taskqueue/v1"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/config"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/queue"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/scheduler"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/server"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dsn := config.Getenv("DATABASE_URL", "postgres://dtq:dtq@localhost:5432/dtq")
	redisAddr := config.Getenv("REDIS_ADDR", "localhost:6379")
	grpcAddr := config.Getenv("GRPC_ADDR", ":50051")
	metricsAddr := config.Getenv("METRICS_ADDR", ":9090")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	st := store.New(pool)
	q := queue.New(rdb)

	// Планировщик
	sched := scheduler.New(q, st)
	go sched.Run(ctx)

	// gRPC сервер
	grpcServer := grpc.NewServer()
	pb.RegisterTaskServiceServer(grpcServer, server.NewTaskServer(st, q))
	pb.RegisterWorkerServiceServer(grpcServer, server.NewWorkerServer(st, q))
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	// HTTP метрики
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("metrics on %s", metricsAddr)
		_ = http.ListenAndServe(metricsAddr, mux)
	}()

	log.Printf("gRPC on %s", grpcAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/daudalobogachiev925/distributed-task-queue/gen/taskqueue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Handler func(ctx context.Context, payload []byte) error

type Worker struct {
	ID           string
	Coordinator  string
	Capabilities []string
	Handlers     map[string]Handler
}

func New(id, coordinator string, handlers map[string]Handler) *Worker {
	return &Worker{
		ID:          id,
		Coordinator: coordinator,
		Handlers:    handlers,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, w.Coordinator,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial coordinator: %w", err)
	}
	defer conn.Close()

	client := pb.NewWorkerServiceClient(conn)

	if _, err := client.Register(ctx, &pb.RegisterRequest{
		WorkerId:     w.ID,
		Capabilities: w.Capabilities,
	}); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	log.Printf("worker %s registered", w.ID)

	go w.heartbeatLoop(ctx, client)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.FetchTask(ctx, &pb.FetchTaskRequest{
			WorkerId:     w.ID,
			Capabilities: w.Capabilities,
			WaitSeconds:  10,
		})
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if !resp.HasTask || resp.Task == nil {
			continue
		}
		w.execute(ctx, client, resp.Task)
	}
}

func (w *Worker) execute(ctx context.Context, client pb.WorkerServiceClient, t *pb.Task) {
	handler, ok := w.Handlers[t.TaskType]
	if !ok {
		_, _ = client.ReportResult(ctx, &pb.ReportResultRequest{
			TaskId:       t.Id,
			WorkerId:     w.ID,
			Success:      false,
			ErrorMessage: fmt.Sprintf("no handler for task_type=%s", t.TaskType),
		})
		return
	}

	log.Printf("executing task %s type=%s", t.Id, t.TaskType)
	execCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	err := handler(execCtx, t.Payload)
	req := &pb.ReportResultRequest{TaskId: t.Id, WorkerId: w.ID, Success: err == nil}
	if err != nil {
		req.ErrorMessage = err.Error()
		log.Printf("task %s failed: %v", t.Id, err)
	} else {
		log.Printf("task %s succeeded", t.Id)
	}
	_, _ = client.ReportResult(ctx, req)
}

func (w *Worker) heartbeatLoop(ctx context.Context, client pb.WorkerServiceClient) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = client.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: w.ID})
		}
	}
}

package server

import (
	"context"
	"time"

	"github.com/google/uuid"
	pb "github.com/daudalobogachiev925/distributed-task-queue/gen/taskqueue/v1"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/metrics"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/queue"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/store"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TaskServer struct {
	pb.UnimplementedTaskServiceServer
	Store *store.Store
	Q     *queue.Queue
}

func NewTaskServer(s *store.Store, q *queue.Queue) *TaskServer {
	return &TaskServer{Store: s, Q: q}
}

func (s *TaskServer) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	id := uuid.New()
	t := &store.Task{
		ID:         id,
		TaskType:   req.TaskType,
		Payload:    req.Payload,
		Priority:   int(req.Priority),
		MaxRetries: int(req.MaxRetries),
	}
	if req.ScheduledAt != nil {
		ts := req.ScheduledAt.AsTime()
		t.ScheduledAt = &ts
	}
	if err := s.Store.Create(ctx, t); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.Q.Enqueue(ctx, id.String(), t.Priority, t.ScheduledAt); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	metrics.TasksSubmitted.Inc()
	return &pb.SubmitTaskResponse{TaskId: id.String()}, nil
}

func (s *TaskServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
	id, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid uuid")
	}
	t, err := s.Store.Get(ctx, id)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if t == nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}
	return toProto(t), nil
}

func (s *TaskServer) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	tasks, total, err := s.Store.List(ctx, req.Status, int(req.Limit), int(req.Offset))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &pb.ListTasksResponse{Total: int32(total)}
	for i := range tasks {
		out.Tasks = append(out.Tasks, toProto(&tasks[i]))
	}
	return out, nil
}

func (s *TaskServer) RetryTask(ctx context.Context, req *pb.RetryTaskRequest) (*emptypb.Empty, error) {
	id, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid uuid")
	}
	t, err := s.Store.Get(ctx, id)
	if err != nil || t == nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}
	if err := s.Store.ResetToPending(ctx, id); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := s.Q.Enqueue(ctx, id.String(), t.Priority, nil); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func toProto(t *store.Task) *pb.Task {
	return &pb.Task{
		Id:           t.ID.String(),
		TaskType:     t.TaskType,
		Payload:      t.Payload,
		Status:       t.Status,
		Priority:     int32(t.Priority),
		RetryCount:   int32(t.RetryCount),
		MaxRetries:   int32(t.MaxRetries),
		ErrorMessage: t.ErrorMessage,
		WorkerId:     t.WorkerID,
		CreatedAt:    timestamppb.New(t.CreatedAt),
		CompletedAt:  tsOrNil(t.CompletedAt),
	}
}

func tsOrNil(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

// ==================== WorkerService ====================

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	Store *store.Store
	Q     *queue.Queue
}

func NewWorkerServer(s *store.Store, q *queue.Queue) *WorkerServer {
	return &WorkerServer{Store: s, Q: q}
}

func (s *WorkerServer) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	return &pb.RegisterResponse{Accepted: true}, nil
}

func (s *WorkerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *WorkerServer) FetchTask(ctx context.Context, req *pb.FetchTaskRequest) (*pb.FetchTaskResponse, error) {
	wait := time.Duration(req.WaitSeconds) * time.Second
	if wait <= 0 {
		wait = 10 * time.Second
	}
	deadline := time.Now().Add(wait)

	notify := s.Q.SubscribeReady(ctx)

	for {
		id, err := s.Q.Dequeue(ctx)
		if err == nil && id != "" {
			uid, _ := uuid.Parse(id)
			t, err := s.Store.Get(ctx, uid)
			if err != nil || t == nil {
				continue
			}
			_ = s.Store.MarkRunning(ctx, uid, req.WorkerId)
			return &pb.FetchTaskResponse{HasTask: true, Task: toProto(t)}, nil
		}
		if err != nil && err != redis.Nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &pb.FetchTaskResponse{HasTask: false}, nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &pb.FetchTaskResponse{HasTask: false}, nil
		case <-notify:
			timer.Stop()
			continue
		case <-timer.C:
			return &pb.FetchTaskResponse{HasTask: false}, nil
		}
	}
}

func (s *WorkerServer) ReportResult(ctx context.Context, req *pb.ReportResultRequest) (*emptypb.Empty, error) {
	id, err := uuid.Parse(req.TaskId)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid uuid")
	}
	if req.Success {
		_ = s.Store.MarkCompleted(ctx, id)
		metrics.TasksCompleted.Inc()
		return &emptypb.Empty{}, nil
	}

	retry, err := s.Store.MarkFailed(ctx, id, req.ErrorMessage)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if retry {
		t, _ := s.Store.Get(ctx, id)
		if t != nil {
			delay := time.Duration(1<<t.RetryCount) * time.Second
			at := time.Now().Add(delay)
			_ = s.Q.Enqueue(ctx, req.TaskId, t.Priority, &at)
		}
	}
	metrics.TasksFailed.WithLabelValues("").Inc()
	return &emptypb.Empty{}, nil
}

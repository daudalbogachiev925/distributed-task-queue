package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	pb "github.com/daudalobogachiev925/distributed-task-queue/gen/taskqueue/v1"
	"github.com/daudalobogachiev925/distributed-task-queue/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type taskJSON struct {
	ID           string `json:"id"`
	TaskType     string `json:"task_type"`
	Status       string `json:"status"`
	Priority     int    `json:"priority"`
	RetryCount   int    `json:"retry_count"`
	MaxRetries   int    `json:"max_retries"`
	ErrorMessage string `json:"error_message"`
	WorkerID     string `json:"worker_id"`
	CreatedAt    string `json:"created_at"`
}

func main() {
	coordAddr := config.Getenv("COORDINATOR_ADDR", "localhost:50051")
	httpAddr := config.Getenv("HTTP_ADDR", ":8080")

	conn, err := grpc.Dial(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewTaskServiceClient(conn)

	http.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit == 0 {
			limit = 100
		}
		resp, err := client.ListTasks(r.Context(), &pb.ListTasksRequest{
			Status: status, Limit: int32(limit),
		})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		out := struct {
			Total int        `json:"total"`
			Tasks []taskJSON `json:"tasks"`
		}{Total: int(resp.Total)}
		for _, t := range resp.Tasks {
			created := ""
			if t.CreatedAt != nil {
				created = t.CreatedAt.AsTime().Format("2006-01-02 15:04:05")
			}
			out.Tasks = append(out.Tasks, taskJSON{
				ID: t.Id, TaskType: t.TaskType, Status: t.Status,
				Priority: int(t.Priority), RetryCount: int(t.RetryCount),
				MaxRetries: int(t.MaxRetries), ErrorMessage: t.ErrorMessage,
				WorkerID: t.WorkerId, CreatedAt: created,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	http.HandleFunc("/api/retry", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "id required", 400)
			return
		}
		_, err := client.RetryTask(r.Context(), &pb.RetryTaskRequest{TaskId: id})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(200)
	})

	http.Handle("/", http.FileServer(http.Dir("web")))

	log.Printf("web on %s", httpAddr)
	log.Fatal(http.ListenAndServe(httpAddr, nil))
}

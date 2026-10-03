package queue

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ReadyKey   = "tq:ready"
	DelayedKey = "tq:delayed"
	NotifyChan = "tq:notify"
)

type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue { return &Queue{rdb: rdb} }

// Enqueue кладёт задачу в ready (приоритетно) либо в delayed
func (q *Queue) Enqueue(ctx context.Context, taskID string, priority int, scheduledAt *time.Time) error {
	if scheduledAt != nil && scheduledAt.After(time.Now()) {
		return q.rdb.ZAdd(ctx, DelayedKey, redis.Z{
			Score:  float64(scheduledAt.Unix()),
			Member: taskID,
		}).Err()
	}
	// Чем выше priority — тем НИЖЕ score (ZPopMin берёт наименьший)
	if err := q.rdb.ZAdd(ctx, ReadyKey, redis.Z{
		Score:  float64(-priority),
		Member: taskID,
	}).Err(); err != nil {
		return err
	}
	// уведомить воркеров
	q.rdb.Publish(ctx, NotifyChan, taskID)
	return nil
}

// Dequeue берёт задачу с максимальным приоритетом
func (q *Queue) Dequeue(ctx context.Context) (string, error) {
	res, err := q.rdb.ZPopMin(ctx, ReadyKey, 1).Result()
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", redis.Nil
	}
	return res[0].Member.(string), nil
}

// PromoteDue перекладывает созревшие delayed → ready
func (q *Queue) PromoteDue(ctx context.Context) (int, error) {
	now := time.Now().Unix()
	ids, err := q.rdb.ZRangeByScore(ctx, DelayedKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now, 10),
	}).Result()
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	pipe := q.rdb.TxPipeline()
	for _, id := range ids {
		pipe.ZRem(ctx, DelayedKey, id)
		pipe.ZAdd(ctx, ReadyKey, redis.Z{Score: 0, Member: id})
	}
	_, err = pipe.Exec(ctx)
	return len(ids), err
}

// SubscribeReady — long-poll: ждём уведомления о новой задаче
func (q *Queue) SubscribeReady(ctx context.Context) <-chan string {
	ch := make(chan string, 16)
	sub := q.rdb.Subscribe(ctx, NotifyChan)
	go func() {
		defer close(ch)
		defer sub.Close()
		for msg := range sub.Channel() {
			select {
			case ch <- msg.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// Depth — размер очереди для метрик
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	return q.rdb.ZCard(ctx, ReadyKey).Result()
}

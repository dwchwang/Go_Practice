package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"order-processing/internal/domain"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(ctx context.Context, addr string) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}

	return &RedisCache{
		client: client,
	}, nil
}

func (r *RedisCache) SetOrder(ctx context.Context, order *domain.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal order: %w", err)
	}

	key := buildOrderKey(order.ID.String())

	err = r.client.Set(ctx, key, data, 10*time.Minute).Err()
	if err != nil {
		return fmt.Errorf("set order cache: %w", err)
	}

	return nil
}

func (r *RedisCache) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	key := buildOrderKey(id)

	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var order domain.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("unmarshal order: %w", err)
	}

	return &order, nil
}

func (r *RedisCache) Close() error {
	return r.client.Close()
}

func buildOrderKey(id string) string {
	return "order:" + id
}

package service

import (
	"context"
	"fmt"
	"mini-ecommerce-redis/internal/model"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type CartService struct {
	rdb *redis.Client
}

func NewCartService(rdb *redis.Client) *CartService{
	return &CartService{
		rdb: rdb,
	}
}

func (s *CartService) AddToCart(ctx context.Context, userID, productID string, quantity int64) error{
	key := fmt.Sprintf("cart:%s", userID)

	return s.rdb.HIncrBy(ctx, key, productID, quantity).Err()
}

func (s *CartService) GetCart(ctx context.Context, userID string) ([]model.CartItem, error){
	key := fmt.Sprintf("cart:%s", userID)

	data, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	items := make([]model.CartItem, 0, len(data))

	for productID, qtyStr := range data {
		qty, err := strconv.ParseInt(qtyStr, 10, 64)
		if err != nil {
			return nil, err
		}

		items = append(items, model.CartItem{
			ProductID: productID,
			Quantity: qty,
		})
	}
	return items, nil
}

func (s *CartService) ClearCart(ctx context.Context, userID string) error {
	cartKey := fmt.Sprintf("cart:%s", userID)
	return s.rdb.Del(ctx, cartKey).Err()
}
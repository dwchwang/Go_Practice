package service

import (
	"context"
	"encoding/json"
	"fmt"
	"mini-ecommerce-redis/internal/model"
	"mini-ecommerce-redis/internal/store"
	"time"

	"github.com/redis/go-redis/v9"
)

type ProductService struct {
	rdb *redis.Client
}

func NewProductService(rdb *redis.Client) *ProductService{
	return &ProductService{
		rdb: rdb,
	}
}

func (s *ProductService) GetProducts(ctx context.Context, page int) ([]model.Product, bool, error){
	cacheKey := fmt.Sprintf("cache:products:page:%d", page)

	// hit cache
	cached, err := s.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var products []model.Product
		if err := json.Unmarshal([]byte(cached), &products); err != nil{
			return nil, false, err
		}
		return products, true, nil
	}
	// cache miss
	if err != redis.Nil {
		return nil, false, err
	}

	products := store.Products
	
	bytes, err := json.Marshal(products)
	if err != nil {
		return nil, false, err
	}
	if err := s.rdb.Set(ctx, cacheKey, bytes, 60*time.Second).Err(); err != nil {
		return nil, false, err
	}
	return products, false, nil
}
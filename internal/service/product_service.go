package service

import (
	"context"
	"encoding/json"
	"fmt"
	"mini-ecommerce-redis/internal/model"
	"mini-ecommerce-redis/internal/repository"
	"time"

	"github.com/redis/go-redis/v9"
)

type ProductService struct {
	rdb         *redis.Client
	productRepo *repository.ProductRepository
}

func NewProductService(
	rdb *redis.Client,
	productRepo *repository.ProductRepository,
) *ProductService {
	return &ProductService{
		rdb:         rdb,
		productRepo: productRepo,
	}
}

func (s *ProductService) GetProducts(ctx context.Context, page int, limit int) ([]model.Product, bool, error) {
	if page <= 0 {
		page = 1
	}

	if limit <= 0 {
		limit = 10
	}

	cacheKey := fmt.Sprintf("cache:products:page:%d:limit:%d", page, limit)

	// hit cache
	cached, err := s.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var products []model.Product
		if err := json.Unmarshal([]byte(cached), &products); err != nil {
			return nil, false, err
		}
		return products, true, nil
	}
	// cache miss
	if err != redis.Nil {
		return nil, false, err
	}

	products, err := s.productRepo.List(ctx, page, limit)
	if err != nil {
		return nil, false, err
	}

	bytes, err := json.Marshal(products)
	if err != nil {
		return nil, false, err
	}
	if err := s.rdb.Set(ctx, cacheKey, bytes, 60*time.Second).Err(); err != nil {
		return nil, false, err
	}
	return products, false, nil
}
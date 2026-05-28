package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"order-processing/internal/cache"
	"order-processing/internal/domain"
	"order-processing/internal/repository"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

var (
	ErrInvalidOrderID = errors.New("invalid order id")
	ErrOrderNotFound  = errors.New("order not found")
)

type CreateOrderInput struct {
	UserID    string
	ProductID string
	Amount    float64
}

type OrderService struct {
	orderRepo  *repository.OrderRepository
	redisCache *cache.RedisCache
}

func NewOrderService(orderRepo *repository.OrderRepository, redisCache *cache.RedisCache) *OrderService {
	return &OrderService{
		orderRepo:  orderRepo,
		redisCache: redisCache,
	}
}

func (s *OrderService) CreateOrder(ctx context.Context, input CreateOrderInput) (*domain.Order, error) {
	order := &domain.Order{
		ID:        uuid.New(),
		UserID:    input.UserID,
		ProductID: input.ProductID,
		Amount:    input.Amount,
		Status:    domain.StatusPending,
	}

	if err := s.orderRepo.Create(ctx, order); err != nil {
		return nil, fmt.Errorf("create order: %w", err)
	}

	// Cache lỗi không nên làm fail request.
	// Source of truth vẫn là PostgreSQL.
	if err := s.redisCache.SetOrder(ctx, order); err != nil {
		log.Printf("[OrderService] set redis cache error: %v", err)
	}

	return order, nil
}

func (s *OrderService) GetOrderByID(ctx context.Context, idStr string) (*domain.Order, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, ErrInvalidOrderID
	}

	// 1. Đọc Redis trước
	order, err := s.redisCache.GetOrder(ctx, id.String())
	if err == nil {
		log.Printf("[OrderService] Cache HIT order_id=%s", id.String())
		return order, nil
	}

	// Redis không có key thì là cache miss, không phải lỗi nghiêm trọng.
	if !errors.Is(err, redis.Nil) {
		log.Printf("[OrderService] redis get error: %v", err)
	}

	log.Printf("[OrderService] Cache MISS order_id=%s", id.String())

	// 2. Cache miss -> đọc PostgreSQL
	order, err = s.orderRepo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}

		return nil, fmt.Errorf("get order from db: %w", err)
	}

	// 3. Warm cache lại Redis
	if err := s.redisCache.SetOrder(ctx, order); err != nil {
		log.Printf("[OrderService] warm redis cache error: %v", err)
	}

	return order, nil
}

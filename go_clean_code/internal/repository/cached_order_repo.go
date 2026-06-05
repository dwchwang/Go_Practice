package repository

import (
	"context"
	"errors"
	"log"

	"order-processing/internal/domain"
	"order-processing/internal/domain/ports"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// CachedOrderRepository là Proxy pattern: bọc OrderRepository (DB) và OrderCache (Redis),
// xử lý cache-aside trong suốt. Service chỉ gọi ports.OrderStore interface,
// không biết cache tồn tại.
type CachedOrderRepository struct {
	dbRepo *OrderRepository // Real Subject — DB access
	cache  ports.OrderCache // Redis (interface)
}

func NewCachedOrderRepository(dbRepo *OrderRepository, cache ports.OrderCache) *CachedOrderRepository {
	return &CachedOrderRepository{
		dbRepo: dbRepo,
		cache:  cache,
	}
}

// GetByID: cache-aside pattern.
// Cache hit → return ngay.
// Cache miss → đọc DB → warm cache → return.
func (r *CachedOrderRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	// 1. Thử đọc từ cache
	order, err := r.cache.GetOrder(ctx, id.String())
	if err == nil {
		log.Printf("[CachedOrderRepo] Cache HIT order_id=%s", id.String())
		return order, nil
	}

	if !errors.Is(err, redis.Nil) {
		log.Printf("[CachedOrderRepo] cache error (non-fatal): %v", err)
	}

	// 2. Cache miss → đọc từ DB
	log.Printf("[CachedOrderRepo] Cache MISS order_id=%s", id.String())
	order, err = r.dbRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// 3. Warm cache (lỗi cache không làm fail request)
	if err := r.cache.SetOrder(ctx, order); err != nil {
		log.Printf("[CachedOrderRepo] warm cache error: %v", err)
	}

	return order, nil
}

// CreateWithOutbox: delegate xuống DB repo, sau đó warm cache.
func (r *CachedOrderRepository) CreateWithOutbox(ctx context.Context, order *domain.Order) error {
	if err := r.dbRepo.CreateWithOutbox(ctx, order); err != nil {
		return err
	}

	// Cache lỗi không nên làm fail request.
	if err := r.cache.SetOrder(ctx, order); err != nil {
		log.Printf("[CachedOrderRepo] cache after create error: %v", err)
	}

	return nil
}

// UpdateStatus: delegate xuống DB repo, đọc lại order đã update, warm cache.
// Lưu ý: Cache consistency là eventual — giữa lúc update DB và warm cache có
// window nhỏ. Trong thực tế, Kafka consumer xử lý message tuần tự theo order_id
// nên concurrent update trên cùng order là rất hiếm. Sau khi method này return,
// cache và DB sẽ nhất quán.
func (r *CachedOrderRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error {
	if err := r.dbRepo.UpdateStatus(ctx, id, status); err != nil {
		return err
	}

	// Đọc lại order sau update để có dữ liệu đầy đủ cho cache
	updatedOrder, err := r.dbRepo.GetByID(ctx, id)
	if err != nil {
		return err // DB read error is fatal (data integrity)
	}

	// Warm cache (non-fatal)
	if err := r.cache.SetOrder(ctx, updatedOrder); err != nil {
		log.Printf("[CachedOrderRepo] cache after update error: %v", err)
	}

	return nil
}

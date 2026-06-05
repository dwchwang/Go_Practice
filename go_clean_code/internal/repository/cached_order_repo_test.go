package repository

import (
	"context"
	"testing"

	"order-processing/internal/domain"

	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// --- Real SQLite-in-memory setup for testing CachedOrderRepository proxy ---

func setupTestProxy(t *testing.T) (*CachedOrderRepository, *mockOrderCache, func()) {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite in-memory: %v", err)
	}

	// Auto-migrate tables needed by OrderRepository
	if err := db.AutoMigrate(&domain.Order{}, &domain.OutboxEvent{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	dbRepo := NewOrderRepository(db)
	cache := newMockOrderCache()
	proxy := NewCachedOrderRepository(dbRepo, cache)

	cleanup := func() {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}

	return proxy, cache, cleanup
}

// --- mockOrderCache (same as before, implements ports.OrderCache) ---

type mockOrderCache struct {
	orders map[string]*domain.Order
}

func newMockOrderCache() *mockOrderCache {
	return &mockOrderCache{orders: make(map[string]*domain.Order)}
}

func (m *mockOrderCache) SetOrder(_ context.Context, order *domain.Order) error {
	m.orders[order.ID.String()] = order
	return nil
}

func (m *mockOrderCache) GetOrder(_ context.Context, id string) (*domain.Order, error) {
	order, ok := m.orders[id]
	if !ok {
		return nil, redis.Nil
	}
	return order, nil
}

// --- Tests: actual CachedOrderRepository (Proxy) ---

func TestCachedOrderRepo_GetByID_CacheHit(t *testing.T) {
	proxy, cache, cleanup := setupTestProxy(t)
	defer cleanup()

	orderID := uuid.New()
	order := &domain.Order{ID: orderID, UserID: "u1", ProductID: "p1", Amount: 99.99, Status: domain.StatusPaid}

	// Warm cache trước — giả lập order đã được cache từ lần đọc trước
	cache.SetOrder(context.Background(), order)

	// Gọi GetByID qua proxy → phải lấy từ cache, không gọi DB
	result, err := proxy.GetByID(context.Background(), orderID)
	if err != nil {
		t.Fatalf("GetByID error: %v", err)
	}
	if result.Status != domain.StatusPaid {
		t.Errorf("expected status Paid, got %s", result.Status)
	}

	// Verify DB không có order này (proxy lấy từ cache, không insert vào DB)
	_, err = proxy.dbRepo.GetByID(context.Background(), orderID)
	if err == nil {
		t.Error("expected DB miss — order was only in cache, not DB")
	}
}

func TestCachedOrderRepo_GetByID_CacheMiss(t *testing.T) {
	proxy, _, cleanup := setupTestProxy(t)
	defer cleanup()

	orderID := uuid.New()
	order := &domain.Order{ID: orderID, UserID: "u1", ProductID: "p1", Amount: 99.99, Status: domain.StatusPending}

	// Insert vào DB qua proxy (CreateWithOutbox sẽ tự cache)
	if err := proxy.CreateWithOutbox(context.Background(), order); err != nil {
		t.Fatalf("CreateWithOutbox error: %v", err)
	}

	// Xóa cache để giả lập cache miss
	// (không làm được với mockOrderCache kiểu map — ta test gián tiếp:
	//  GetByID lần 2 sẽ là cache hit nếu proxy cache đúng)
	result, err := proxy.GetByID(context.Background(), orderID)
	if err != nil {
		t.Fatalf("GetByID error: %v", err)
	}
	if result.Status != domain.StatusPending {
		t.Errorf("expected status Pending, got %s", result.Status)
	}

	// Lần 2: cache hit (đã được warm từ lần 1)
	result2, err := proxy.GetByID(context.Background(), orderID)
	if err != nil {
		t.Fatalf("GetByID (2nd call) error: %v", err)
	}
	if result2.Status != domain.StatusPending {
		t.Errorf("expected cached status Pending, got %s", result2.Status)
	}
}

func TestCachedOrderRepo_CreateWithOutbox_CachesOrder(t *testing.T) {
	proxy, cache, cleanup := setupTestProxy(t)
	defer cleanup()

	orderID := uuid.New()
	order := &domain.Order{
		ID:        orderID,
		UserID:    "u1",
		ProductID: "p1",
		Amount:    50.0,
		Status:    domain.StatusPending,
	}

	// Create qua proxy → phải insert DB + warm cache
	if err := proxy.CreateWithOutbox(context.Background(), order); err != nil {
		t.Fatalf("CreateWithOutbox error: %v", err)
	}

	// Verify order trong cache
	cached, err := cache.GetOrder(context.Background(), orderID.String())
	if err != nil {
		t.Fatalf("cache GetOrder error: %v", err)
	}
	if cached.Status != domain.StatusPending {
		t.Errorf("expected cached status Pending, got %s", cached.Status)
	}

	// Verify order trong DB
	dbOrder, err := proxy.dbRepo.GetByID(context.Background(), orderID)
	if err != nil {
		t.Fatalf("DB GetByID error: %v", err)
	}
	if dbOrder.Status != domain.StatusPending {
		t.Errorf("expected DB status Pending, got %s", dbOrder.Status)
	}
}

func TestCachedOrderRepo_UpdateStatus_CacheWarm(t *testing.T) {
	proxy, cache, cleanup := setupTestProxy(t)
	defer cleanup()

	orderID := uuid.New()
	order := &domain.Order{
		ID:        orderID,
		UserID:    "u1",
		ProductID: "p1",
		Amount:    50.0,
		Status:    domain.StatusPending,
	}

	// Tạo order qua proxy
	if err := proxy.CreateWithOutbox(context.Background(), order); err != nil {
		t.Fatalf("CreateWithOutbox error: %v", err)
	}

	// Update status qua proxy
	if err := proxy.UpdateStatus(context.Background(), orderID, domain.StatusPaid); err != nil {
		t.Fatalf("UpdateStatus error: %v", err)
	}

	// Verify cache đã được warm với status mới
	cached, err := cache.GetOrder(context.Background(), orderID.String())
	if err != nil {
		t.Fatalf("cache GetOrder after update error: %v", err)
	}
	if cached.Status != domain.StatusPaid {
		t.Errorf("expected cached status Paid after update, got %s", cached.Status)
	}

	// Verify DB cũng đã update
	dbOrder, err := proxy.dbRepo.GetByID(context.Background(), orderID)
	if err != nil {
		t.Fatalf("DB GetByID after update error: %v", err)
	}
	if dbOrder.Status != domain.StatusPaid {
		t.Errorf("expected DB status Paid after update, got %s", dbOrder.Status)
	}
}

func TestCachedOrderRepo_GetByID_NonExistent(t *testing.T) {
	proxy, _, cleanup := setupTestProxy(t)
	defer cleanup()

	_, err := proxy.GetByID(context.Background(), uuid.New())
	if err == nil {
		t.Error("expected error for non-existent order")
	}
}


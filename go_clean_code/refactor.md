# Refactor `go_kafka` → `go_clean_code` — Áp Dụng 4 Design Pattern

## Summary

Refactor code trong `go_clean_code` (bản copy của `go_kafka`), giữ nguyên behavior: API, Kafka topics, DB schema, Docker Compose, luồng xử lý order/payment/notification. Mục tiêu: làm 4 pattern hiện rõ trong code thật, không abstraction quá nặng.

Mapping pattern:

| Pattern | Áp dụng vào | Implementation |
|---------|------------|:---:|
| **Singleton** | Config | `config.Get()` với `sync.Once` |
| **Abstract Factory** | Notification channel | `NotificationFactory` → Formatter + Sender (2 family) |
| **Dependency Injection** | Service constructors | Interface nhỏ thay cho concrete type |
| **Proxy** | Cache layer | `CachedOrderRepository` bọc `OrderRepository` |

---

## Phase 1: Singleton — Config (`config.Get()`)

### Mục tiêu

Đảm bảo `*Config` chỉ được khởi tạo 1 lần duy nhất trong toàn process, thread-safe, dùng `sync.Once`.

### Các bước

**1.1** Sửa `internal/config/config.go`:
- Thêm biến package-level: `var instance *Config` + `var once sync.Once`
- Thêm hàm `Get() *Config`:
  - Bên trong gọi `once.Do(func() { instance = load() })`
  - Return `instance`
- Đổi `Load()` thành `load()` (unexported), chỉ expose `Get()`

**1.2** Sửa tất cả file `cmd/*/main.go`:
- Thay `cfg := config.Load()` → `cfg := config.Get()`
- Các file cần sửa:
  - `cmd/order-service/main.go`
  - `cmd/payment-service/main.go`
  - `cmd/notification-service/main.go`
  - `cmd/outbox-relay/main.go`
  - `cmd/kafka-topic-init/main.go`
  - `cmd/kafka-bad-message-test/main.go`

**1.3** Thêm test `internal/config/config_test.go`:
- `TestConfigSingleton`: gọi `Get()` 2 lần, verify cùng pointer
- `TestConfigSingletonConcurrent`: gọi `Get()` từ 10 goroutine, verify tất cả cùng pointer

### File thay đổi

| File | Hành động |
|------|-----------|
| `internal/config/config.go` | Sửa — thêm `Get()`, `sync.Once` |
| `internal/config/config_test.go` | Tạo mới |
| 6 file `cmd/*/main.go` | Sửa — `config.Load()` → `config.Get()` |

### Verify

- `go test ./internal/config/...` — pass
- `go build ./cmd/...` — tất cả binary build được

---

## Phase 2: Abstract Factory — Notification (`NotificationFactory`)

### Mục tiêu

Notification hiện tại chỉ có 1 kiểu giả lập (`log.Printf`). Tách thành Abstract Factory với 2 family, mỗi family tạo 2 product (Formatter + Sender).

### Thiết kế

```
internal/factory/notification/
├── factory.go           // NotificationFactory interface
├── formatter.go         // MessageFormatter interface
├── sender.go            // MessageSender interface
├── email_factory.go     // EmailNotificationFactory: EmailFormatter + EmailSender
└── console_factory.go   // ConsoleNotificationFactory: ConsoleFormatter + ConsoleSender
```

### Interface

```go
// MessageFormatter: tạo nội dung thông báo từ PaymentProcessedEvent
type MessageFormatter interface {
    Format(event domain.PaymentProcessedEvent) string
}

// MessageSender: gửi thông báo đến user
type MessageSender interface {
    Send(ctx context.Context, userID string, content string) error
}

// NotificationFactory: Abstract Factory
type NotificationFactory interface {
    CreateFormatter() MessageFormatter
    CreateSender() MessageSender
}
```

### Các bước

**2.1** Tạo package `internal/factory/notification/`:

**2.1.1** `internal/factory/notification/factory.go`:
- Định nghĩa `NotificationFactory` interface với 2 method: `CreateFormatter()` và `CreateSender()`

**2.1.2** `internal/factory/notification/formatter.go`:
- Định nghĩa `MessageFormatter` interface với method `Format(event domain.PaymentProcessedEvent) string`

**2.1.3** `internal/factory/notification/sender.go`:
- Định nghĩa `MessageSender` interface với method `Send(ctx context.Context, userID string, content string) error`

**2.1.4** `internal/factory/notification/email_factory.go`:
- `EmailNotificationFactory` struct (rỗng, hoặc có config SMTP sau này)
- Implement `CreateFormatter()` → trả về `EmailFormatter`
- Implement `CreateSender()` → trả về `EmailSender`
- `EmailFormatter.Format()`: format kiểu `[EMAIL] Subject: Order {orderID} - Payment {status}`
- `EmailSender.Send()`: `log.Printf("[EmailSender] Sending email to user %s: %s", userID, content)` + `time.Sleep(200ms)` giả lập

**2.1.5** `internal/factory/notification/console_factory.go`:
- `ConsoleNotificationFactory` struct
- Implement `CreateFormatter()` → trả về `ConsoleFormatter`
- Implement `CreateSender()` → trả về `ConsoleSender`
- `ConsoleFormatter.Format()`: format kiểu `[CONSOLE] User {userID}: Order {orderID} is {status}`
- `ConsoleSender.Send()`: `log.Printf("[ConsoleSender] %s", content)` (không sleep, instant)

**2.2** Sửa `internal/service/notification_service.go`:
- Trường `processedRepo` giữ nguyên
- Thêm trường `factory notification.NotificationFactory` (interface!)
- Constructor `NewNotificationService(processedRepo, factory, consumerGroup)`
- Trong `HandlePaymentProcessed`:
  - Sau khi parse event và check idempotency
  - Gọi `formatter := s.factory.CreateFormatter()`
  - Gọi `content := formatter.Format(event)`
  - Gọi `sender := s.factory.CreateSender()`
  - Gọi `sender.Send(ctx, event.UserID, content)`
  - Bỏ `time.Sleep(300 * time.Millisecond)` cũ

**2.3** Sửa `cmd/notification-service/main.go`:
- Import `"order-processing/internal/factory/notification"`
- Tạo factory: `notifFactory := &notification.EmailNotificationFactory{}` (hoặc `ConsoleNotificationFactory{}`)
- Truyền vào `service.NewNotificationService(processedRepo, notifFactory, consumerGroup)`

**2.4** Thêm test `internal/factory/notification/email_factory_test.go`:
- Test `EmailFormatter.Format()` trả về string chứa order ID
- Test `EmailSender.Send()` không lỗi

### File thay đổi

| File | Hành động |
|------|-----------|
| `internal/factory/notification/factory.go` | Tạo mới |
| `internal/factory/notification/formatter.go` | Tạo mới |
| `internal/factory/notification/sender.go` | Tạo mới |
| `internal/factory/notification/email_factory.go` | Tạo mới |
| `internal/factory/notification/console_factory.go` | Tạo mới |
| `internal/factory/notification/email_factory_test.go` | Tạo mới |
| `internal/service/notification_service.go` | Sửa — inject NotificationFactory |
| `cmd/notification-service/main.go` | Sửa — tạo factory, truyền vào service |

### Verify

- `go test ./internal/factory/notification/...` — pass
- Chạy notification-service, gửi 1 order → log ra `[EmailSender] Sending email...`
- Đổi `main.go` sang `ConsoleNotificationFactory` → log ra `[ConsoleSender]...`

---

## Phase 3: Dependency Injection — Interface thay Concrete Type

### Mục tiêu

Service hiện tại nhận concrete type (`*repository.OrderRepository`, `*cache.RedisCache`, `*appkafka.Producer`). Chuyển sang nhận interface nhỏ, giúp test được và giảm coupling.

### Thiết kế Interface

Tạo package `internal/domain/ports/` chứa các interface:

```go
// ports/order_store.go
type OrderStore interface {
    CreateWithOutbox(ctx context.Context, order *domain.Order) error
    GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error)
    UpdateStatus(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error
}

// ports/event_publisher.go
type EventPublisher interface {
    PublishEvent(ctx context.Context, topic string, key string, payload interface{}) error
}

// ports/processed_message_store.go
type ProcessedMessageStore interface {
    IsProcessed(ctx context.Context, messageID, consumerGroup string) (bool, error)
    MarkProcessed(ctx context.Context, msg *domain.ProcessedMessage) error
}

// ports/order_cache.go
type OrderCache interface {
    SetOrder(ctx context.Context, order *domain.Order) error
    GetOrder(ctx context.Context, id string) (*domain.Order, error)
}
```

### Các bước

**3.1** Tạo package `internal/domain/ports/`:

**3.1.1** `internal/domain/ports/order_store.go` — định nghĩa `OrderStore` interface
**3.1.2** `internal/domain/ports/event_publisher.go` — định nghĩa `EventPublisher` interface
**3.1.3** `internal/domain/ports/processed_message_store.go` — định nghĩa `ProcessedMessageStore` interface
**3.1.4** `internal/domain/ports/order_cache.go` — định nghĩa `OrderCache` interface

**3.2** Verify concrete types đã implicitly implement interface:
- `*repository.OrderRepository` → `OrderStore` ✅ (có CreateWithOutbox, GetByID, UpdateStatus)
- `*appkafka.Producer` → `EventPublisher` ✅ (có PublishEvent)
- `*repository.ProcessedMessageRepository` → `ProcessedMessageStore` ✅ (có IsProcessed, MarkProcessed)
- `*cache.RedisCache` → `OrderCache` ✅ (có SetOrder, GetOrder)

**3.3** Sửa `internal/service/order_service.go`:
- Đổi field `orderRepo *repository.OrderRepository` → `orderStore ports.OrderStore`
- Đổi field `redisCache *cache.RedisCache` → `orderCache ports.OrderCache`
- Đổi constructor param tương ứng
- Cập nhật import (bỏ `repository`, `cache`, thêm `ports`)
- Logic bên trong không thay đổi

**3.4** Sửa `internal/service/payment_service.go`:
- Đổi field `orderRepo` → `orderStore ports.OrderStore`
- Đổi field `processedRepo` → `processedStore ports.ProcessedMessageStore`
- Đổi field `redisCache` → `orderCache ports.OrderCache`
- Đổi field `producer` → `eventPublisher ports.EventPublisher`
- Đổi constructor param tương ứng
- Cập nhật import
- Logic bên trong không thay đổi

**3.5** Sửa `internal/service/notification_service.go`:
- Đổi field `processedRepo` → `processedStore ports.ProcessedMessageStore`
- Đổi constructor param tương ứng
- Cập nhật import
- Logic bên trong không thay đổi

**3.6** Sửa `cmd/*/main.go` — không cần thay đổi logic, chỉ cần verify compile:
- Các concrete type vẫn được tạo như cũ, truyền vào service qua constructor
- Go compiler tự động coi concrete type là implementation của interface

**3.7** Thêm test cho DI (không cần Docker):

**3.7.1** `internal/service/payment_service_test.go`:
- Tạo `MockOrderStore`, `MockProcessedMessageStore`, `MockOrderCache`, `MockEventPublisher` (struct với field func)
- Test `HandleOrderCreated` với mock → verify gọi đúng method
- Test idempotency: `IsProcessed` return true → không gọi `UpdateStatus`

### File thay đổi

| File | Hành động |
|------|-----------|
| `internal/domain/ports/order_store.go` | Tạo mới |
| `internal/domain/ports/event_publisher.go` | Tạo mới |
| `internal/domain/ports/processed_message_store.go` | Tạo mới |
| `internal/domain/ports/order_cache.go` | Tạo mới |
| `internal/service/order_service.go` | Sửa — dùng interface |
| `internal/service/payment_service.go` | Sửa — dùng interface |
| `internal/service/notification_service.go` | Sửa — dùng interface |
| `internal/service/payment_service_test.go` | Tạo mới |
| 6 file `cmd/*/main.go` | Kiểm tra compile (có thể không cần sửa) |

### Verify

- `go build ./...` — tất cả compile
- `go test ./internal/service/...` — test DI pass (mock không cần Docker)
- `go test ./...` — không break test cũ

---

## Phase 4: Proxy — CachedOrderRepository

### Mục tiêu

Di chuyển toàn bộ cache-aside logic từ Service layer vào Proxy `CachedOrderRepository`. Service chỉ gọi `OrderStore` interface, không biết cache tồn tại.

### Thiết kế

```
CachedOrderRepository (implements ports.OrderStore)
├── wraps OrderRepository (DB)
├── wraps OrderCache (Redis)
│
├── GetByID()     → cache hit: return cached
│                 → cache miss: DB.GetByID() → warm cache → return
├── CreateWithOutbox() → DB.CreateWithOutbox() → warm cache → return
└── UpdateStatus()     → DB.UpdateStatus() → DB.GetByID() → warm cache → return
```

### Các bước

**4.1** Tạo `internal/repository/cached_order_repo.go`:

```go
type CachedOrderRepository struct {
    dbRepo    *OrderRepository    // concrete DB repo
    cache     ports.OrderCache    // interface (RedisCache hoặc mock)
}

func NewCachedOrderRepository(dbRepo *OrderRepository, cache ports.OrderCache) *CachedOrderRepository

// GetByID: cache-aside
func (r *CachedOrderRepository) GetByID(ctx, id) (*Order, error) {
    // 1. Try cache
    order, err := r.cache.GetOrder(ctx, id.String())
    if err == nil {
        return order, nil
    }
    if !errors.Is(err, redis.Nil) {
        log.Printf("cache error (non-fatal): %v", err)
    }
    // 2. Cache miss → DB
    order, err = r.dbRepo.GetByID(ctx, id)
    if err != nil {
        return nil, err
    }
    // 3. Warm cache (non-fatal)
    if err := r.cache.SetOrder(ctx, order); err != nil {
        log.Printf("warm cache error: %v", err)
    }
    return order, nil
}

// CreateWithOutbox: delegate DB, then cache
func (r *CachedOrderRepository) CreateWithOutbox(ctx, order) error {
    if err := r.dbRepo.CreateWithOutbox(ctx, order); err != nil {
        return err
    }
    if err := r.cache.SetOrder(ctx, order); err != nil {
        log.Printf("cache after create error: %v", err)
    }
    return nil
}

// UpdateStatus: delegate DB, read back, then cache
func (r *CachedOrderRepository) UpdateStatus(ctx, id, status) error {
    if err := r.dbRepo.UpdateStatus(ctx, id, status); err != nil {
        return err
    }
    updatedOrder, err := r.dbRepo.GetByID(ctx, id)
    if err != nil {
        return err  // DB read error is fatal (data integrity)
    }
    if err := r.cache.SetOrder(ctx, updatedOrder); err != nil {
        log.Printf("cache after update error: %v", err)
    }
    return nil
}
```

**4.2** Sửa `internal/service/order_service.go`:
- **Xóa toàn bộ logic cache** trong `GetOrderByID` và `CreateOrder`
- Chỉ gọi `s.orderStore.GetByID()` / `s.orderStore.CreateWithOutbox()`
- Không import `redis`, không tự check cache

**4.3** Sửa `internal/service/payment_service.go`:
- **Xóa logic cache** trong `HandleOrderCreated`:
  - Sau `UpdateStatus`, bỏ phần `GetByID` + `SetOrder` (Proxy đã làm)
  - Chỉ gọi `s.orderStore.UpdateStatus()`
- Lưu ý: `PaymentService` cần đọc order để log (sau UpdateStatus). Có thể gọi `GetByID` qua Proxy (sẽ tự cache) hoặc không đọc lại nếu không cần.

**4.4** Sửa `cmd/order-service/main.go`:
- Tạo `CachedOrderRepository` thay vì `OrderRepository`:
  ```go
  dbRepo := repository.NewOrderRepository(db)
  orderRepo := repository.NewCachedOrderRepository(dbRepo, redisCache)
  orderService := service.NewOrderService(orderRepo)  // chỉ cần OrderStore
  ```
- Bỏ truyền `redisCache` vào `OrderService` (vì service không cần cache nữa)

**4.5** Sửa `cmd/payment-service/main.go`:
- Tương tự: tạo `CachedOrderRepository` rồi truyền vào `PaymentService`
- `PaymentService` không nhận `redisCache` nữa (chỉ nhận `OrderStore`)

**4.6** Thêm test `internal/repository/cached_order_repo_test.go`:
- `TestCachedOrderRepo_GetByID_CacheHit`: mock cache return order → không gọi DB
- `TestCachedOrderRepo_GetByID_CacheMiss`: mock cache return redis.Nil → gọi DB → gọi cache.SetOrder
- `TestCachedOrderRepo_CreateWithOutbox`: DB thành công → gọi cache.SetOrder
- `TestCachedOrderRepo_UpdateStatus_CacheWarm`: DB thành công → gọi cache.SetOrder với order đã update

### File thay đổi

| File | Hành động |
|------|-----------|
| `internal/repository/cached_order_repo.go` | Tạo mới |
| `internal/repository/cached_order_repo_test.go` | Tạo mới |
| `internal/service/order_service.go` | Sửa — xóa cache logic |
| `internal/service/payment_service.go` | Sửa — xóa cache logic |
| `cmd/order-service/main.go` | Sửa — wire CachedOrderRepository |
| `cmd/payment-service/main.go` | Sửa — wire CachedOrderRepository |

### Verify

- `go test ./internal/repository/...` — pass (cache hit/miss/warm test)
- Demo: tạo order → GET order (log "Cache MISS" lần đầu, "Cache HIT" lần sau)
- Payment update → log "cache after update" từ Proxy

---

## Phase 5: Tổng kiểm tra & Documentation

### Các bước

**5.1** Smoke test với Docker:
- `docker compose up -d`
- Chạy `go run ./cmd/kafka-topic-init`
- Chạy `go run ./cmd/order-service`
- Chạy `go run ./cmd/outbox-relay`
- Chạy `go run ./cmd/payment-service`
- Chạy `go run ./cmd/notification-service`
- Gửi `POST /orders` → verify order chuyển pending → paid/cancelled
- GET `/orders/:id` → verify cache hit lần 2

**5.2** Chạy toàn bộ test:
- `go test ./...` — tất cả pass
- `go vet ./...` — không warning

**5.3** Viết `internal/DESIGN_PATTERNS.md`:
- Mô tả ngắn từng pattern: nằm ở file nào, tại sao dùng ở đó
- Kèm code snippet minh họa
- Bảng mapping pattern → file

**5.4** Cập nhật `README.md`:
- Thêm section "Design Patterns" ở cuối
- Link đến `internal/DESIGN_PATTERNS.md`

### File thay đổi

| File | Hành động |
|------|-----------|
| `internal/DESIGN_PATTERNS.md` | Tạo mới |
| `README.md` | Sửa — thêm section Design Patterns |

---

## Tổng quan thứ tự thực hiện

```
Phase 1 (Singleton) ─────────────────────────────┐
                                                  ├── Không phụ thuộc nhau,
Phase 2 (Abstract Factory - Notification) ────────┤   làm song song được
                                                  │
Phase 3 (Dependency Injection) ──────────────────┘
       │
       └── Phase 4 (Proxy) ← PHỤ THUỘC Phase 3 (cần interface ports.OrderStore đã có)
              │
              └── Phase 5 (Tổng kiểm tra + Docs) ← PHỤ THUỘC tất cả phase trên
```

### Khuyến nghị thứ tự:

1. **Phase 1 + Phase 2 song song** (2 người hoặc làm lần lượt) — không conflict
2. **Phase 3** — cần Phase 1 (config singleton) đã xong, nhưng không cần Phase 2
3. **Phase 4** — PHẢI có Phase 3 trước (cần `ports.OrderStore` interface)
4. **Phase 5** — sau cùng

---

## Public Interfaces / Behavior — KHÔNG ĐỔI

- HTTP API: `POST /orders`, `GET /orders/:id`
- Kafka topics: `order.created`, `order.created.DLQ`, `order.payment.processed`, `order.payment.processed.DLQ`
- DB schema: không đổi
- Redis key format: `order:{id}` (TTL 10 phút)
- Docker Compose: không đổi
- Runtime commands: `go run ./cmd/...`

---

## Assumptions

- Tất cả refactor trong thư mục `go_clean_code/`
- Giữ mức refactor vừa phải: code dễ hiểu cho học design pattern
- Notification family chỉ giả lập (log.Printf), không tích hợp SDK thật
- Cache-aside logic chuyển hoàn toàn vào Proxy, service không còn import `cache` package

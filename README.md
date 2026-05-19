# Mini E-commerce Redis

Mini e-commerce API viết bằng Go, Gin và Redis. Project mô phỏng các luồng cơ bản của một hệ thống bán hàng nhỏ: đăng nhập, xem sản phẩm, thêm giỏ hàng, tạo đơn hàng, giới hạn request, bảng xếp hạng điểm và thông báo đơn hàng qua Redis Pub/Sub.

## Công nghệ sử dụng

- Go
- Gin Web Framework
- Redis
- go-redis/v9
- Docker Compose

## Chức năng chính

- Đăng nhập bằng tài khoản demo và lưu session trong Redis.
- Middleware xác thực bằng `Authorization: Bearer <session_id>`.
- Rate limit theo user: tối đa 10 request/phút cho các route `/api`.
- Cache danh sách sản phẩm trong Redis trong 60 giây.
- Giỏ hàng lưu bằng Redis Hash.
- Checkout tạo đơn hàng, kiểm tra tồn kho và trừ tồn kho trong Redis.
- Distributed lock khi tạo đơn để tránh checkout đồng thời cho cùng một user.
- Cộng điểm leaderboard sau khi order thành công.
- Publish/Subscribe notification khi có order mới.

## Cấu trúc thư mục

```text
mini-ecommerce-redis/
├── cmd/api/main.go                  # Entry point, khởi tạo Redis, DI, routes
├── internal/handler/                # HTTP handlers
├── internal/middleware/             # Auth middleware, rate limit middleware
├── internal/model/                  # Struct model
├── internal/service/                # Business logic dùng Redis
├── internal/store/mock.go           # Mock users và products
├── docker-compose.yaml              # Redis service
├── go.mod
└── go.sum
```

## Luồng hệ thống

### 1. Authentication

Client gọi `POST /auth/login` với email và password. Nếu hợp lệ, hệ thống tạo `session_id`, lưu thông tin user vào Redis Hash và trả session cho client.

Redis keys:

```text
session:<session_id>       # Hash: user_id, email, name
user:<user_id>:sessions    # Set: danh sách session_id của user
```

Session ban đầu hết hạn sau 60 phút. Khi request hợp lệ đi qua auth middleware, session được gia hạn theo sliding expiration 30 phút.

Tài khoản demo:

```text
email: demo@example.com
password: 123456
```

### 2. Product cache

Route `GET /api/products?page=1` lấy danh sách product. Lần đầu cache miss thì đọc từ mock store, marshal JSON và lưu Redis trong 60 giây. Các lần sau đọc từ Redis.

Redis key:

```text
cache:products:page:<page>
```

### 3. Cart

Giỏ hàng dùng Redis Hash, trong đó field là `product_id`, value là số lượng.

Redis key:

```text
cart:<user_id>
```

Khi gọi `POST /api/cart/add`, hệ thống dùng `HINCRBY` để tăng số lượng sản phẩm trong giỏ.

### 4. Order

Khi gọi `POST /api/order`, hệ thống thực hiện:

1. Tạo distributed lock theo user bằng `SETNX lock:order:<user_id>`.
2. Lấy cart từ Redis.
3. Kiểm tra tồn kho từng sản phẩm.
4. Trừ tồn kho bằng `DECRBY`.
5. Xóa cart sau khi checkout thành công.
6. Cộng 10 điểm vào leaderboard.
7. Publish message vào channel notification.
8. Release lock bằng Lua script để đảm bảo chỉ xóa đúng lock do request hiện tại tạo.

Redis keys:

```text
lock:order:<user_id>
inventory:<product_id>
leaderboard
notif:orders
```

Inventory demo được seed khi server start:

```text
inventory:p1 = 10
inventory:p2 = 15
inventory:p3 = 5
```

### 5. Leaderboard

Leaderboard dùng Redis Sorted Set. Mỗi order thành công cộng 10 điểm cho user.

Redis key:

```text
leaderboard
```

### 6. Notification

Khi order thành công, service publish message vào Redis channel `notif:orders`. Server cũng subscribe channel này bằng goroutine và log notification ra console.

## Chạy project

### 1. Chạy Redis

```bash
docker compose up -d
```

Redis chạy tại:

```text
localhost:6379
```

### 2. Chạy API

```bash
go run ./cmd/api
```

Server chạy tại:

```text
http://localhost:8080
```

### 3. Kiểm tra health

```bash
curl http://localhost:8080/ping
```

Response:

```json
{
  "message": "pong"
}
```

Kiểm tra Redis:

```bash
curl http://localhost:8080/redis-ping
```

Response:

```json
{
  "redis": "PONG"
}
```

## API endpoints

### Login

```http
POST /auth/login
Content-Type: application/json
```

Request:

```json
{
  "email": "demo@example.com",
  "password": "123456"
}
```

Response:

```json
{
  "session_id": "<session_id>",
  "token_type": "Bearer"
}
```

Các API dưới đây cần header:

```http
Authorization: Bearer <session_id>
```

### Lấy thông tin user hiện tại

```http
GET /api/me
```

Response:

```json
{
  "user_id": "1",
  "email": "demo@example.com",
  "name": "Demo User"
}
```

### Lấy danh sách sản phẩm

```http
GET /api/products?page=1
```

Response:

```json
{
  "cache_hit": false,
  "data": [
    {
      "id": "p1",
      "name": "iPhone 15",
      "description": "Apple smartphone",
      "price": 999,
      "stock": 10,
      "category": "phone"
    }
  ],
  "page": 1
}
```

### Thêm sản phẩm vào giỏ hàng

```http
POST /api/cart/add
Content-Type: application/json
Authorization: Bearer <session_id>
```

Request:

```json
{
  "product_id": "p1",
  "quantity": 2
}
```

Response:

```json
{
  "message": "product added to cart"
}
```

### Xem giỏ hàng

```http
GET /api/cart
Authorization: Bearer <session_id>
```

Response:

```json
{
  "items": [
    {
      "product_id": "p1",
      "quantity": 2
    }
  ]
}
```

### Tạo đơn hàng

```http
POST /api/order
Authorization: Bearer <session_id>
```

Response:

```json
{
  "message": "order created",
  "order": {
    "user_id": "1",
    "items": [
      {
        "product_id": "p1",
        "quantity": 2
      }
    ]
  }
}
```

### Cộng điểm leaderboard thủ công

```http
POST /api/leaderboard/add
Content-Type: application/json
Authorization: Bearer <session_id>
```

Request:

```json
{
  "score": 5
}
```

Response:

```json
{
  "message": "score added"
}
```

### Xem leaderboard

```http
GET /api/leaderboard?limit=10
Authorization: Bearer <session_id>
```

Response:

```json
{
  "items": [
    {
      "user_id": "1",
      "score": 10,
      "rank": 1
    }
  ]
}
```

## Ví dụ test nhanh bằng curl

Login:

```bash
curl -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"demo@example.com\",\"password\":\"123456\"}"
```

Gán session vào biến môi trường:

```bash
export TOKEN="<session_id>"
```

Windows PowerShell:

```powershell
$env:TOKEN = "<session_id>"
```

Lấy products:

```bash
curl http://localhost:8080/api/products?page=1 \
  -H "Authorization: Bearer $TOKEN"
```

Thêm vào cart:

```bash
curl -X POST http://localhost:8080/api/cart/add \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"product_id\":\"p1\",\"quantity\":2}"
```

Tạo order:

```bash
curl -X POST http://localhost:8080/api/order \
  -H "Authorization: Bearer $TOKEN"
```

Xem leaderboard:

```bash
curl http://localhost:8080/api/leaderboard?limit=10 \
  -H "Authorization: Bearer $TOKEN"
```

## Redis data structure đang dùng

| Tính năng | Redis structure | Key |
| --- | --- | --- |
| Session | Hash | `session:<session_id>` |
| User sessions | Set | `user:<user_id>:sessions` |
| Product cache | String | `cache:products:page:<page>` |
| Cart | Hash | `cart:<user_id>` |
| Inventory | String number | `inventory:<product_id>` |
| Order lock | String | `lock:order:<user_id>` |
| Leaderboard | Sorted Set | `leaderboard` |
| Notification | Pub/Sub Channel | `notif:orders` |

## Ghi chú hiện tại

- Product data và user data đang là mock trong `internal/store/mock.go`.
- Tham số `page` của products hiện được dùng để tạo cache key, chưa phân trang dữ liệu thực tế.
- Inventory được set lại khi server khởi động.
- Project hiện chưa có database chính; Redis đang được dùng cho session, cache, cart, inventory demo, lock, leaderboard và pub/sub.

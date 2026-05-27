# Mini E-commerce Redis

Mini e-commerce API viết bằng Go, Gin, PostgreSQL, GORM và Redis. Project mô phỏng một hệ thống bán hàng nhỏ với đăng nhập, session, cache sản phẩm, giỏ hàng, checkout, quản lý tồn kho, leaderboard, rate limit và notification qua Redis Pub/Sub.

## Công nghệ sử dụng

- Go 1.25
- Gin Web Framework
- PostgreSQL 16
- GORM
- Redis 8
- go-redis/v9
- Docker Compose

## Kiến trúc hiện tại

PostgreSQL là nguồn dữ liệu chính cho các dữ liệu cần lưu bền vững:

- Users
- Products
- Product stock
- Orders
- Order items

Redis được dùng cho dữ liệu tạm, cache và realtime:

- Session đăng nhập
- Rate limit theo user
- Cache danh sách sản phẩm
- Cart
- Distributed lock khi checkout theo user
- Leaderboard
- Pub/Sub notification khi tạo order

## Chức năng chính

- Đăng nhập bằng user demo từ PostgreSQL.
- Lưu session trong Redis và xác thực bằng `Authorization: Bearer <session_id>`.
- Sliding expiration cho session khi request hợp lệ.
- Rate limit các route `/api`: tối đa 10 request/phút/user.
- Lấy danh sách sản phẩm từ PostgreSQL, cache Redis trong 60 giây.
- Giỏ hàng lưu bằng Redis Hash.
- Checkout tạo order và order items trong PostgreSQL.
- Kiểm tra và trừ tồn kho bằng PostgreSQL transaction + row lock `FOR UPDATE`.
- Redis lock theo user để hạn chế checkout đồng thời từ cùng một user.
- Xóa cart sau khi order commit thành công.
- Cộng điểm leaderboard sau khi order thành công.
- Publish notification vào Redis channel `notif:orders`.

## Cấu trúc thư mục

```text
mini-ecommerce-redis/
├── cmd/api/main.go                  # Entry point, DI, routes, connect Redis/PostgreSQL
├── internal/config/                 # Load env, connect PostgreSQL bằng GORM
├── internal/database/               # AutoMigrate và seed dữ liệu demo
├── internal/handler/                # HTTP handlers
├── internal/middleware/             # Auth middleware, rate limit middleware
├── internal/model/                  # GORM models và response models
├── internal/repository/             # Repository layer thao tác PostgreSQL
├── internal/service/                # Business logic dùng Redis/PostgreSQL
├── docker-compose.yaml              # Redis và PostgreSQL
├── go.mod
└── go.sum
```



## Redis keys đang dùng

| Tính năng | Redis structure | Key |
| --- | --- | --- |
| Session | Hash | `session:<session_id>` |
| User sessions | Set | `user:<user_id>:sessions` |
| Rate limit | String counter | `rl:<user_id>:<window>` |
| Product cache | String JSON | `cache:products:page:<page>:limit:<limit>` |
| Cart | Hash | `cart:<user_id>` |
| Order lock | String | `lock:order:<user_id>` |
| Leaderboard | Sorted Set | `leaderboard` |
| Notification | Pub/Sub Channel | `notif:orders` |

## PostgreSQL tables

GORM AutoMigrate tạo các bảng chính:

| Table | Vai trò |
| --- | --- |
| `users` | Tài khoản người dùng |
| `products` | Danh sách sản phẩm và tồn kho |
| `orders` | Đơn hàng |
| `order_items` | Chi tiết sản phẩm trong đơn hàng |


## Luồng checkout

Khi gọi `POST /api/order`, hệ thống thực hiện:

1. Tạo Redis lock `lock:order:<user_id>` bằng `SETNX`.
2. Lấy cart từ Redis Hash `cart:<user_id>`.
3. Mở PostgreSQL transaction.
4. Với từng sản phẩm trong cart, lock row product bằng `FOR UPDATE`.
5. Kiểm tra stock và trừ stock trong cùng transaction.
6. Tạo order và order items trong PostgreSQL.
7. Commit transaction.
8. Xóa cart trong Redis.
9. Cộng 10 điểm vào Redis Sorted Set `leaderboard`.
10. Publish message vào Redis channel `notif:orders`.
11. Release Redis lock bằng Lua script để chỉ xóa đúng lock của request hiện tại.

## Kiến thức Redis áp dụng

- `Hash`: lưu session data tại `session:<session_id>` và cart tại `cart:<user_id>`.
- `Set`: lưu danh sách session của user tại `user:<user_id>:sessions`.
- `String`: lưu product cache dạng JSON tại `cache:products:page:<page>:limit:<limit>`.
- `String counter`: làm rate limit theo user tại `rl:<user_id>:<window>`.
- `Sorted Set`: làm leaderboard tại key `leaderboard`.
- `Pub/Sub`: gửi notification khi tạo order qua channel `notif:orders`.
- `TTL`: tự động hết hạn session, product cache, rate limit counter và order lock.
- `Cache-aside pattern`: đọc Redis trước, cache miss thì query PostgreSQL rồi ghi lại Redis.
- `Atomic increment`: dùng `HINCRBY` để tăng số lượng sản phẩm trong cart và `INCR` cho rate limit.
- `Distributed lock`: dùng `SETNX` cho `lock:order:<user_id>` để hạn chế checkout trùng.
- `Lua script`: release lock an toàn, chỉ xóa lock nếu đúng request đang giữ lock.
- `Pipeline`: gom các lệnh tạo session như `HSET`, `EXPIRE`, `SADD` để giảm round-trip tới Redis.

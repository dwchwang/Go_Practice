package service

import (
	"context"
	"errors"
	"fmt"
	"mini-ecommerce-redis/internal/model"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type OrderService struct {
	rdb         *redis.Client
	cartService *CartService
}

func NewOrderService(
	rdb *redis.Client,
	cartService *CartService,
) *OrderService {
	return &OrderService{
		rdb:         rdb,
		cartService: cartService,
	}
}

func (s *OrderService) CreateOrder(ctx context.Context, userID string) (*model.Order, error) {
	// distributed lock => Tranh 2 req checkout cung luc -> inventory bi tru 2 lan
	lockKey := fmt.Sprintf("lock:order:%s", userID)
	lockValue := uuid.NewString()

	locked, err := s.rdb.SetNX(ctx, lockKey, lockValue, 30*time.Second).Result()

	if err != nil {
		return nil, err
	}

	if !locked {
		return nil, errors.New("another order is processing")
	}

	// release Lock bang LUA => an toan, ko dung DEL lockKey vi co the RC voi GET
	defer func() {
		lua := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
		`

		s.rdb.Eval(context.Background(), lua, []string{lockKey}, lockValue)
	}()

	// lay cart
	items, err := s.cartService.GetCart(ctx, userID)
	if err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return nil, errors.New("cart is empty")
	}

	// check inventory
	for _, item := range items {
		iventoryKey := fmt.Sprintf("inventory:%s", item.ProductID)

		stock, err := s.rdb.Get(ctx, iventoryKey).Int64()
		if err != nil {
			return nil, err
		}

		if stock < item.Quantity {
			return nil, fmt.Errorf(
				"insufficient stock for product %s",
				item.ProductID,
			)
		}
	}

	// decrease inventory
	for _, item := range items {
		inventoryKey := fmt.Sprintf("inventory:%s", item.ProductID)

		if err := s.rdb.DecrBy(
			ctx,
			inventoryKey,
			item.Quantity,
		).Err(); err != nil {
			return nil, err
		}
	}

	// clear cart
	cartKey := fmt.Sprintf("cart:%s", userID)

	if err := s.rdb.Del(ctx, cartKey).Err(); err != nil {
		return nil, err
	}

	// return order
	order := &model.Order{
		UserID: userID,
		Items: items,
	}

	return order, nil
}

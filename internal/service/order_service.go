package service

import (
	"context"
	"errors"
	"fmt"
	"mini-ecommerce-redis/internal/model"
	"mini-ecommerce-redis/internal/repository"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type OrderService struct {
	rdb                 *redis.Client
	db                  *gorm.DB
	cartService         *CartService
	productRepo         *repository.ProductRepository
	orderRepo           *repository.OrderRepository
	leaderboardService  *LeaderboardService  // order thanh cong -> cong diem
	notificationService *NotificationService // thong bao msg order thanh cong
}

func NewOrderService(
	rdb *redis.Client,
	db *gorm.DB,
	cartService *CartService,
	productRepo *repository.ProductRepository,
	orderRepo *repository.OrderRepository,
	leaderboardService *LeaderboardService,
	notificationService *NotificationService,
) *OrderService {
	return &OrderService{
		rdb:                 rdb,
		db:                  db,
		cartService:         cartService,
		productRepo:         productRepo,
		orderRepo:           orderRepo,
		leaderboardService:  leaderboardService,
		notificationService: notificationService,
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

	var createdOrder *model.Order

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var total float64
		orderItems := make([]model.OrderItem, 0, len(items))

		for _, item := range items {
			product, err := s.productRepo.FindByIDForUpdate(ctx, tx, item.ProductID)
			if err != nil {
				return err
			}

			if product.Stock < item.Quantity {
				return fmt.Errorf("insufficient stock for product %s", item.ProductID)
			}

			total += product.Price * float64(item.Quantity)

			if err := s.productRepo.DecreaseStock(ctx, tx, item.ProductID, item.Quantity); err != nil {
				return err
			}

			orderItems = append(orderItems, model.OrderItem{
				ProductID: item.ProductID,
				Quantity:  item.Quantity,
				UnitPrice: product.Price,
			})
		}

		order := &model.Order{
			UserID:      userID,
			Status:      "created",
			TotalAmount: total,
			Items:       orderItems,
		}

		if err := s.orderRepo.Create(ctx, tx, order); err != nil {
			return err
		}

		createdOrder = order
		return nil
	})

	if err != nil {
		return nil, err
	}

	if err := s.cartService.ClearCart(ctx, userID); err != nil {
		return nil, err
	}

	if err := s.leaderboardService.AddScore(ctx, userID, 10); err != nil {
		return nil, err
	}

	if err := s.notificationService.PublishOrderCreated(ctx, userID); err != nil {
		return nil, err
	}

	return createdOrder, nil
}

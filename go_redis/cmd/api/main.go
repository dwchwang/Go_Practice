package main

import (
	"context"
	"log"
	"mini-ecommerce-redis/internal/config"
	"mini-ecommerce-redis/internal/database"
	"mini-ecommerce-redis/internal/handler"
	"mini-ecommerce-redis/internal/middleware"
	"mini-ecommerce-redis/internal/repository"
	"mini-ecommerce-redis/internal/service"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	// connect PostgreSQL
	db := config.ConnectDatabase(cfg)

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Postgres DB instance failed:", err)
	}
	defer sqlDB.Close()

	if err := sqlDB.Ping(); err != nil {
		log.Fatal("Postgres ping failed:", err)
	}

	// seed data
	if err := database.Seed(db); err != nil {
		log.Fatal("Seed failed:", err)
	}

	// connect Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	// connect gin golang
	r := gin.Default()

	// DI
	userRepo := repository.NewUserRepository(db)
	authService := service.NewAuthService(rdb, userRepo)
	authHandler := handler.NewAuthHandler(authService)

	productRepo := repository.NewProductRepository(db)
	productService := service.NewProductService(rdb, productRepo)
	productHandler := handler.NewProductHandler(productService)

	cartService := service.NewCartService(rdb)
	cartHandler := handler.NewCartHandler(cartService)

	leaderboardService := service.NewLeaderboardService(rdb)
	leaderboardHandler := handler.NewLeaderboardHandler(leaderboardService)

	notificationService := service.NewNotificationService(rdb)

	orderRepo := repository.NewOrderRepository(db)
	orderService := service.NewOrderService(
		rdb,
		db,
		cartService,
		productRepo,
		orderRepo,
		leaderboardService,
		notificationService,
	)
	orderHandler := handler.NewOrderHandler(orderService)
	// routes
	r.POST("/auth/login", authHandler.Login)

	protected := r.Group("/api")

	// middleware
	protected.Use(middleware.AuthMiddleware(authService))
	protected.Use(middleware.RateLimitMiddleware(rdb, 10, time.Minute))

	protected.GET("/me", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"user_id": c.GetString("user_id"),
			"email":   c.GetString("email"),
			"name":    c.GetString("name"),
		})
	})

	protected.GET("/products", productHandler.GetProducts)

	protected.POST("/cart/add", cartHandler.AddToCart)
	protected.GET("/cart", cartHandler.GetCart)

	protected.POST("/order", orderHandler.CreateOrder)

	protected.POST("/leaderboard/add", leaderboardHandler.AddScore) // test thu cong
	protected.GET("/leaderboard", leaderboardHandler.GetLeaderboard)

	// start subscribe bang goroutine
	go notificationService.SubscribeOrderNotifications(ctx)

	// ping gin
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// ping PostgreSQL
	r.GET("/postgres-ping", func(c *gin.Context) {
		sqlDB, err := db.DB()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		if err := sqlDB.Ping(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"postgres": "OK",
		})
	})

	// ping redis
	r.GET("/redis-ping", func(c *gin.Context) {
		result, err := rdb.Ping(c.Request.Context()).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"redis": result,
		})
	})

	log.Println("Server running at :8080")
	if err := r.Run(cfg.ServerAddr); err != nil {
		log.Fatal(err)
	}

}

package main

import (
	"context"
	"log"
	"mini-ecommerce-redis/internal/handler"
	"mini-ecommerce-redis/internal/middleware"
	"mini-ecommerce-redis/internal/service"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	r := gin.Default()

	// DI
	authService := service.NewAuthService(rdb)
	authHandler := handler.NewAuthHandler(authService)

	productService := service.NewProductService(rdb)
	productHandler := handler.NewProductHandler(productService)

	cartService := service.NewCartService(rdb)
	cartHandler := handler.NewCartHandler(cartService)

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

	// ping
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

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
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}

}

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"order-processing/internal/cache"
	"order-processing/internal/config"
	"order-processing/internal/database"
	"order-processing/internal/handler"
	"order-processing/internal/repository"
	"order-processing/internal/routes"
	"order-processing/internal/service"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg := config.Get()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := database.NewPostgres(cfg.PostgresDSN)
	if err != nil {
		log.Fatal("[OrderService] connect postgres error:", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("[OrderService] get sql db error:", err)
	}
	defer sqlDB.Close()

	redisCache, err := cache.NewRedisCache(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatal("[OrderService] connect redis error:", err)
	}
	defer redisCache.Close()

	//order
	dbRepo := repository.NewOrderRepository(db)
	orderRepo := repository.NewCachedOrderRepository(dbRepo, redisCache)
	orderService := service.NewOrderService(orderRepo)
	orderHandler := handler.NewOrderHandler(orderService)

	router := gin.Default()

	routes.RegisterOrderRoutes(router, orderHandler)

	server := &http.Server{
		Addr:    ":" + cfg.ServerPort,
		Handler: router,
	}

	go func() {
		log.Printf("[OrderService] Listening on :%s", cfg.ServerPort)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("[OrderService] listen error:", err)
		}
	}()

	<-ctx.Done()

	log.Println("[OrderService] shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[OrderService] shutdown error: %v", err)
	}

	log.Println("[OrderService] shutdown complete")
}

package routes

import (
	"order-processing/internal/handler"

	"github.com/gin-gonic/gin"
)

func RegisterOrderRoutes(router *gin.Engine, orderHandler *handler.OrderHandler) {
	router.POST("/orders", orderHandler.CreateOrder)
	router.GET("/orders/:id", orderHandler.GetOrderByID)
}

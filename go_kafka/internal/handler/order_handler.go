package handler

import (
	"errors"
	"net/http"
	"order-processing/internal/service"

	"github.com/gin-gonic/gin"
)

type OrderHandler struct {
	orderService *service.OrderService
}

func NewOrderHandler(orderService *service.OrderService) *OrderHandler {
	return &OrderHandler{
		orderService: orderService,
	}
}

type CreateOrderRequest struct {
	UserID    string  `json:"user_id" binding:"required"`
	ProductID string  `json:"product_id" binding:"required"`
	Amount    float64 `json:"amount" binding:"required,gt=0"`
}

func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req CreateOrderRequest

	if err := c.ShouldBindBodyWithJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":  "invalid request body",
			"detail": err.Error(),
		})
		return
	}

	order, err := h.orderService.CreateOrder(c.Request.Context(), service.CreateOrderInput{
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Amount:    req.Amount,
	})

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":  "invalid request body",
			"detail": err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, order)
}

func (h *OrderHandler) GetOrderByID(c *gin.Context) {
	id := c.Param("id")

	order, err := h.orderService.GetOrderByID(c.Request.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidOrderID):
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid order id",
			})
			return

		case errors.Is(err, service.ErrOrderNotFound):
			c.JSON(http.StatusNotFound, gin.H{
				"error": "order not found",
			})
			return

		default:
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "cannot get order",
			})
			return
		}
	}

	c.JSON(http.StatusOK, order)
}

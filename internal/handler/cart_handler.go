package handler

import (
	"mini-ecommerce-redis/internal/service"
	"net/http"

	"github.com/gin-gonic/gin"
)

type CartHandler struct {
	cartService *service.CartService
}

func NewCartHandler(cartService *service.CartService) *CartHandler{
	return &CartHandler{
		cartService: cartService,
	}
}

type AddToCartRequest struct{
	ProductID string `json:"product_id" binding:"required"`
	Quantity  int64  `json:"quantity" binding:"required,min=1"`
}

func (h *CartHandler) AddToCart(c *gin.Context){
	userID := c.GetString("user_id")

	var req AddToCartRequest
	if err := c.ShouldBindBodyWithJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if err := h.cartService.AddToCart(c.Request.Context(), userID, req.ProductID, req.Quantity); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"message": "product added to cart",
	})
}

func (h *CartHandler) GetCart(c *gin.Context){
	userID := c.GetString("user_id")
	
	items, err := h.cartService.GetCart(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": items,
	})
}
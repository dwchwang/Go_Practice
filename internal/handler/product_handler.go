package handler

import (
	"mini-ecommerce-redis/internal/service"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type ProductHandler struct {
	productService *service.ProductService
}

func NewProductHandler(productService *service.ProductService) *ProductHandler {
	return &ProductHandler{
		productService: productService,
	}
}

func (h *ProductHandler) GetProducts(c *gin.Context) {
	pageStr := c.DefaultQuery("page", "1")

	page, err := strconv.Atoi(pageStr)

	if err != nil || page <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid page",
		})
		return
	}

	products, cacheHit, err := h.productService.GetProducts(c.Request.Context(), page)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data": products,
		"page": page,
		"cache_hit": cacheHit,
	})
}

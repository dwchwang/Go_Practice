package handler

import (
	"mini-ecommerce-redis/internal/service"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type LeaderboardHandler struct {
	leaderboardService *service.LeaderboardService
}

func NewLeaderboardHandler(leaderboardService *service.LeaderboardService) *LeaderboardHandler {
	return &LeaderboardHandler{
		leaderboardService: leaderboardService,
	}
}

type AddScoreRequest struct {
	Score float64 `json:"score" binding:"required"`
}

func (h *LeaderboardHandler) AddScore(c *gin.Context) {
	userID := c.GetString("user_id")

	var req AddScoreRequest

	if err := c.ShouldBindBodyWithJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if err := h.leaderboardService.AddScore(c.Request.Context(), userID, req.Score); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "score added",
	})
}

func (h *LeaderboardHandler) GetLeaderboard(c *gin.Context){
	limitStr := c.DefaultQuery("limit", "10")

	limit, err := strconv.ParseInt(limitStr, 10, 64)

	if err != nil || limit <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid limit",
		})
		return
	}

	items, err := h.leaderboardService.GetTop(c.Request.Context(), limit)
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
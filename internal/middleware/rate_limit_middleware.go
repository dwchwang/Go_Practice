package middleware

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func RateLimitMiddleware(rdb *redis.Client, limit int64, window time.Duration) gin.HandlerFunc{
	return func(c *gin.Context){
		userID := c.GetString("user_id")
		if userID == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "missing user context",
			})
			c.Abort()
			return
		}
		nowWindow := time.Now().Unix() / int64(window.Seconds())
		key := fmt.Sprintf("rl:%s:%d", userID, nowWindow)

		count, err := rdb.Incr(c.Request.Context(), key).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "rate limit error",
			})
			c.Abort()
			return
		}

		if count == 1 {
			if err := rdb.Expire(c.Request.Context(), key, window).Err(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": "rate limit expire now",
				})
				c.Abort()
				return
			}
		}
		ttl, _ := rdb.TTL(c.Request.Context(), key).Result()
		c.Header("X-RateLimit-Limit", fmt.Sprintf("%d", limit))
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", max(0, limit-count)))
		c.Header("X-RateLimit-Reset", fmt.Sprintf("%d", int(ttl.Seconds())))

		if count > limit {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
				"retry_after": int(ttl.Seconds()),
			})
			c.Abort()
			return
		}
		c.Next()
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

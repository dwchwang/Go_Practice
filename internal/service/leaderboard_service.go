package service

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type LeaderboardItem struct {
	UserID string  `json:"user_id"`
	Score  float64 `json:"score"`
	Rank   int64   `json:"rank"`
}

type LeaderboardService struct {
	rdb *redis.Client
}

func NewLeaderboardService(rdb *redis.Client) *LeaderboardService {
	return &LeaderboardService{
		rdb: rdb,
	}
}

func (s *LeaderboardService) AddScore(ctx context.Context, userID string, score float64) error {
	return s.rdb.ZIncrBy(ctx, "leaderboard", score, userID).Err()
} 

func (s *LeaderboardService) GetTop(ctx context.Context, limit int64) ([]LeaderboardItem, error){
	results, err := s.rdb.ZRevRangeWithScores(ctx, "leaderboard", 0, limit-1).Result()
	if err != nil {
		return nil, err 
	}
	items := make([]LeaderboardItem, 0, len(results))

	for index, result := range results {
		items = append(items, LeaderboardItem{
			UserID: result.Member.(string),
			Score: result.Score,
			Rank: int64(index + 1),
		})
	}

	return items, nil
}
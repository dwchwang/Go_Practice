package service

import (
	"context"
	"errors"
	"fmt"
	"mini-ecommerce-redis/internal/store"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type AuthService struct {
	rdb *redis.Client
}

func NewAuthService(rdb *redis.Client) *AuthService {
	return &AuthService{
		rdb: rdb,
	}
}

type SessionData struct {
	UserID string
	Email string
	Name string
}

// login
func (s *AuthService) Login(ctx context.Context,email, password string,) (string, error) {
	user, ok := store.Users[email]
	if !ok {
		return "", errors.New("Invalid credentials")
	}

	if user.Password != password {
		return "", errors.New("Invalid credentials")
	}
	
	sessionID := uuid.NewString()
	sessionKey := fmt.Sprintf("session:%s", sessionID)
	userSessionKey := fmt.Sprintf("user:%s:sessions",user.ID)

	// 1. HSet sessionKey => dung pipeline 
	pipe := s.rdb.TxPipeline()

	pipe.HSet(ctx, sessionKey, map[string]interface{}{
		"user_id": user.ID,
		"email": user.Email,
		"name": user.Name,
	})
	// 2. expire sessionKey
	pipe.Expire(ctx,sessionKey, 60*time.Minute)

	// 3. SADD userSessionKey sessionID
	pipe.SAdd(ctx, userSessionKey, sessionID)

	// 4. expire userSessionKey 
	pipe.Expire(ctx, userSessionKey, 24*time.Hour)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return "", err
	}
	return sessionID, nil
}

// Get session data
func (s *AuthService) GetSession(ctx context.Context, sessionID string) (*SessionData, error) {
	sessionKey := fmt.Sprintf("session:%s", sessionID)

	data, err := s.rdb.HGetAll(ctx, sessionKey).Result()
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, errors.New("invalid or expired session")
	}

	// sliding expiration
	if err := s.rdb.Expire(ctx, sessionKey, 30*time.Minute).Err(); err != nil {
		return nil, err
	}
	return &SessionData{
		UserID: data["user_id"],
		Email: data["email"],
		Name: data["name"],
	}, nil
}
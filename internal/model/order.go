package model

type Order struct {
	UserID string     `json:"user_id"`
	Items  []CartItem `json:"items"`
}

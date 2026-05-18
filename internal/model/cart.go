package model

type CartItem struct{
	ProductID string `json:"product_id"`
	Quantity  int64  `json:"quantity"`
}
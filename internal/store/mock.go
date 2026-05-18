package store

import "mini-ecommerce-redis/internal/model"

var Users = map[string]model.User{
	"demo@example.com": {
		ID:       "1",
		Email:    "demo@example.com",
		Password: "123456",
		Name:     "Demo User",
	},
}

var Products = []model.Product{
	{
		ID:          "p1",
		Name:        "iPhone 15",
		Description: "Apple smartphone",
		Price:       999,
		Stock:       10,
		Category:    "phone",
	},
	{
		ID:          "p2",
		Name:        "Samsung Galaxy S24",
		Description: "Samsung smartphone",
		Price:       899,
		Stock:       15,
		Category:    "phone",
	},
	{
		ID:          "p3",
		Name:        "MacBook Pro",
		Description: "Apple laptop",
		Price:       1999,
		Stock:       5,
		Category:    "laptop",
	},
}

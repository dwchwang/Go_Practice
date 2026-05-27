package database

import (
	"mini-ecommerce-redis/internal/model"

	"gorm.io/gorm"
)

func Seed(db *gorm.DB) error {
	user := model.User{
		ID:           "00000000-0000-0000-0000-000000000001",
		Email:        "demo@example.com",
		PasswordHash: "123456",
		Name:         "Demo User",
	}

	if err := db.FirstOrCreate(&user, model.User{Email: user.Email}).Error; err != nil {
		return err
	}

	products := []model.Product{
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

	for _, product := range products {
		if err := db.FirstOrCreate(&product, model.Product{ID: product.ID}).Error; err != nil {
			return err
		}
	}

	return nil
}

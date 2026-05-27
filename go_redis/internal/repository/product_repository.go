package repository

import (
	"context"
	"mini-ecommerce-redis/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ProductRepository struct {
	db *gorm.DB
}

func NewProductRepository(db *gorm.DB) *ProductRepository {
	return &ProductRepository{
		db: db,
	}
}

func (r *ProductRepository) List(ctx context.Context, page, limit int) ([]model.Product, error) {
	var products []model.Product
	offset := (page - 1) * limit

	err := r.db.WithContext(ctx).
		Order("id ASC").
		Limit(limit).
		Offset(offset).
		Find(&products).Error

	return products, err
}

func (r *ProductRepository) FindByIDForUpdate(ctx context.Context, tx *gorm.DB, productID string) (*model.Product, error) {
	var product model.Product
	err := tx.WithContext(ctx).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", productID).
		First(&product).Error

	if err != nil {
		return nil, err
	}
	return &product, nil
}

func (r *ProductRepository) DecreaseStock(ctx context.Context, tx *gorm.DB, productID string, quantity int64) error {
	return tx.WithContext(ctx).
		Model(&model.Product{}).
		Where("id = ?", productID).
		UpdateColumn("stock", gorm.Expr("stock - ?", quantity)).
		Error
}
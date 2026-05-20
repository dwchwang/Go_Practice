package config

import (
	"context"
	"fmt"
	"log"
	"mini-ecommerce-redis/internal/database"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var DB *gorm.DB

func ConnectDatabase(cfg Config) *gorm.DB {
	// Sử dụng DatabaseDSN đã được hàm Load() gom tụ lại
	dsn := cfg.DatabaseDSN

	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: false, // Giữ nguyên cách đặt tên bảng số nhiều (ví dụ: users)
		},
	}

	db, err := gorm.Open(postgres.Open(dsn), gormConfig)
	if err != nil {
		log.Fatal("Lỗi kết nối DB: ", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Lỗi lấy đối tượng sql.DB: ", err)
	}

	// Cấu hình Connection Pool giống như bài trước của bạn
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// Ping thử nghiệm
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		log.Fatal("Lỗi Ping tới database mini_ecommerce: ", err)
	}

	fmt.Println("Đã kết nối thành công tới database mini_ecommerce!")

	if err := database.AutoMigrate(db); err != nil {
		log.Fatal("Lỗi AutoMigrate: ", err)
	}
	fmt.Println("Đã AutoMigrate thành công!")

	DB = db
	return db
}

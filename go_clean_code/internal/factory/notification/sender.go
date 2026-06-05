package notification

import "context"

// MessageSender gửi thông báo đến user qua một kênh cụ thể.
// Mỗi kênh (Email, Console, SMS...) có cách gửi riêng.
type MessageSender interface {
	Send(ctx context.Context, userID string, content string) error
}

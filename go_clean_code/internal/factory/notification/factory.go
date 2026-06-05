package notification

// NotificationFactory là Abstract Factory: tạo family các object liên quan đến notification.
// Mỗi concrete factory tạo ra một cặp Formatter + Sender cho một kênh thông báo cụ thể.
type NotificationFactory interface {
	CreateFormatter() MessageFormatter
	CreateSender() MessageSender
}

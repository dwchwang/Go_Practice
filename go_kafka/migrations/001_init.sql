CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS orders (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      VARCHAR(100) NOT NULL,
    product_id   VARCHAR(100) NOT NULL,
    amount       DECIMAL(10,2) NOT NULL,
    status       VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id   VARCHAR(100) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    payload        JSONB NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'pending',
    retry_count    INT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id     VARCHAR(150) NOT NULL,
    consumer_group VARCHAR(150) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition      INT NOT NULL,
    offset_value   BIGINT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (message_id, consumer_group)
);

CREATE INDEX IF NOT EXISTS idx_processed_messages_consumer_group
ON processed_messages(consumer_group);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);

CREATE INDEX IF NOT EXISTS idx_outbox_status
ON outbox(status)
WHERE status = 'pending';
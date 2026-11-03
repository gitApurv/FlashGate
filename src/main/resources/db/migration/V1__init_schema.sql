-- 1. Flash Sale Campaigns Table
CREATE TABLE campaigns (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    total_stock INT NOT NULL CHECK (total_stock > 0),
    available_stock INT NOT NULL CHECK (available_stock >= 0),
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    version BIGINT NOT NULL DEFAULT 0, -- Used for JPA @Version Optimistic Locking (v2)
    sale_starts_at TIMESTAMPTZ NOT NULL,
    sale_ends_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_stock_bounds CHECK (available_stock <= total_stock),
    CONSTRAINT chk_sale_window CHECK (sale_ends_at > sale_starts_at)
);

-- 2. Reservations Table (Order + Idempotency Ledger)
CREATE TABLE reservations (
    id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES campaigns(id),
    user_id UUID NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL UNIQUE,
    status VARCHAR(24) NOT NULL, -- PENDING_PAYMENT, CONFIRMED, EXPIRED, CANCELLED
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_campaign_user UNIQUE (campaign_id, user_id)
);

-- Partial index optimized for the Expiration Polling Worker (SKIP LOCKED)
CREATE INDEX idx_reservations_pending_expiry 
    ON reservations (expires_at) 
    WHERE status = 'PENDING_PAYMENT';

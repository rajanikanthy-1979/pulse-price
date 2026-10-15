-- ============================================================================
-- PulsePrice Phase 4: Metasearch Trigram Indexes & Price Drop Alerts Schema
-- ============================================================================

-- 1. Enable PostgreSQL Trigram Extension for Fast Metasearch
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. GIN Trigram Indexes for Substring and Typo-Tolerant Product Search
CREATE INDEX IF NOT EXISTS idx_canonical_products_trgm_title
    ON canonical_products USING gin (title gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_canonical_products_trgm_brand
    ON canonical_products USING gin (brand gin_trgm_ops);

-- Compound index for combined title and brand search ranking
CREATE INDEX IF NOT EXISTS idx_canonical_products_title_brand_trgm
    ON canonical_products USING gin ((title || ' ' || COALESCE(brand, '')) gin_trgm_ops);

-- 3. Price Alert Subscriptions Table
CREATE TABLE IF NOT EXISTS price_alert_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_product_id UUID NOT NULL REFERENCES canonical_products(id) ON DELETE CASCADE,
    user_email VARCHAR(255) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    target_price NUMERIC(12, 2),
    percentage_drop_threshold NUMERIC(5, 2),
    notification_channel VARCHAR(50) NOT NULL DEFAULT 'EMAIL',
    destination VARCHAR(500) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    cooldown_hours INT NOT NULL DEFAULT 24,
    last_triggered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for lightning-fast subscription lookup during scrape ingestion
CREATE INDEX IF NOT EXISTS idx_alert_subs_canonical_active
    ON price_alert_subscriptions (canonical_product_id, active)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_alert_subs_user_email
    ON price_alert_subscriptions (user_email);

-- 4. Price Alert Execution History Table (Audit Log)
CREATE TABLE IF NOT EXISTS price_alert_history (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES price_alert_subscriptions(id) ON DELETE CASCADE,
    canonical_product_id UUID NOT NULL REFERENCES canonical_products(id) ON DELETE CASCADE,
    retailer_product_id UUID NOT NULL REFERENCES retailer_products(id) ON DELETE CASCADE,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trigger_type VARCHAR(50) NOT NULL,
    old_price NUMERIC(12, 2),
    new_price NUMERIC(12, 2) NOT NULL,
    price_drop_amount NUMERIC(12, 2),
    price_drop_percentage NUMERIC(5, 2),
    channel VARCHAR(50) NOT NULL,
    destination VARCHAR(500) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    PRIMARY KEY (triggered_at, id)
);

-- 5. Convert price_alert_history into TimescaleDB Hypertable
SELECT create_hypertable(
    'price_alert_history',
    by_range('triggered_at', INTERVAL '30 days'),
    if_not_exists => TRUE
);

-- 6. Hypertable Compression and Retention Policy
ALTER TABLE price_alert_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'subscription_id, trigger_type, status',
    timescaledb.compress_orderby = 'triggered_at DESC'
);

SELECT add_compression_policy('price_alert_history', INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_retention_policy('price_alert_history', INTERVAL '90 days', if_not_exists => TRUE);

-- 7. Hourly Alert Delivery Summary Continuous Aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_alert_delivery_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', triggered_at) AS bucket_hour,
    trigger_type,
    channel,
    status,
    COUNT(*) AS total_dispatched,
    AVG(price_drop_percentage)::NUMERIC(5, 2) AS avg_percentage_drop
FROM price_alert_history
GROUP BY bucket_hour, trigger_type, channel, status
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'hourly_alert_delivery_summary',
    start_offset => INTERVAL '30 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

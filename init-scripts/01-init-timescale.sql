-- Enable TimescaleDB and UUID extensions
CREATE
EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE
EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS canonical_products(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gtin varchar(14) unique,
    brand varchar(255),
    model varchar(255),
    title varchar(500) not null,
    created_at timestampz not null default now(),
    updated_at timestampz not null default now()
);

CREATE INDEX IF NOT EXISTS idx_canonical_products_brand_model
    ON canonical_products (LOWER(brand), LOWER(model));

CREATE TABLE IF NOT EXISTS retailer_products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_product_id UUID NOT NULL REFERENCES canonical_products(id) ON DELETE CASCADE,
  retailer VARCHAR(100) NOT NULL,
  retailer_sku VARCHAR(255),
  url TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

create index if not exists idx_retailer_products_canonical on retailer_products(canonical_product_id);
create index if not exists idx_retailer_products_retailer_sku on retailer_products (retailer, retailer_sku);

CREATE TABLE IF NOT EXISTS price_history (
    recorded_at TIMESTAMPTZ NOT NULL,
    retailer_product_id UUID NOT NULL REFERENCES retailer_products(id) ON DELETE CASCADE,
    canonical_product_id UUID NOT NULL REFERENCES canonical_products(id) ON DELETE CASCADE,
    price NUMERIC(12, 2) NOT NULL,
    original_price NUMERIC(12, 2),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    in_stock BOOLEAN NOT NULL DEFAULT TRUE
);

SELECT create_hypertable(
       'price_history',
       by_range('recorded_at', INTERVAL '7 days'),
       if_not_exists => TRUE
);

create index if not exists idx_price_history_retailer_time on price_history(retailer_product_id, recorded_at desc);
create index if not exists idx_price_history_canonical_time on price_history(canonical_product_id, recorded_at desc);

alter table price_history set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'retailer_product_id, canonical_product_id',
    timescaledb.compress_orderby = 'recorded_at DESC'
);

select add_compression_policy('price_history', interval '14 days', if_not_exists => true);

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_price_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', recorded_at) AS bucket_day,
    canonical_product_id,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price)::NUMERIC(12, 2) AS avg_price,
    COUNT(*) AS sample_count
FROM price_history
GROUP BY bucket_day, canonical_product_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('daily_price_summary',
       start_offset => INTERVAL '1 month',
       end_offset => INTERVAL '1 hour',
       schedule_interval => INTERVAL '1 hour',
       if_not_exists => TRUE
);



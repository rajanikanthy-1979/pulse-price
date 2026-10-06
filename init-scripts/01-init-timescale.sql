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

create table if not exists price_history6 (
    recorded_at TIMESTAMPZ not null,
    retailer_product_id uuid not null references retailer_products(id) on delete cascade,
    canonical_product_id uuid not null references canonical_products(id) on delete cascade,
    privce numeric(12, 2) not null,
    original_price numeric(12, 2),
    currency varchar(3) not null default 'USD',
    in_stock boolean not null default true
);

select create_hypertable(
       'price_history',
       by_range('recorded_at', internal '7 days'),
       if_not_exists => true
);

create index if not exists idx_price_history_retailer_time on price_history(retailer_product_id, recorded_at desc);
create index if not exists idx_price_history_canonical_time on price_history(canonical_product_id, recorded_at desc);

alter table price_history set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'retailer_product_id, canonical_product_id',
    timescaledb.compress_orderby = 'recorded_at DESC'
);

select add_compression_policy('price_history', interval '14 days', if_not_exists => true);

create materialized view if not exists daily_price_summary
with (timescaledb.continous) as
select
    time_bucket('1 day', recorded_at) as bucket_day,
    canonical_product_id,
    min(price) as min_price,
    max(price) as max_price,
    avg(price)::numeric(12, 2) as avg_price,
    count(*) as sample_count
from price_history
group by bucket_day, canonical_product_id
with no data;

select add_continous_aggregate_policy('daily_price_summary',
       start_offset => interval '1 month',
       end_offset => interval '1 hour',
       schedule_interval => interval '1 hour',
       if_not_exists => true
       )


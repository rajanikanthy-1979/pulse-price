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




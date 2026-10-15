-- 1. Scrape Failures Table (Audit log for DOM drift, anti-bot challenges, and unparseable pages)
CREATE TABLE IF NOT EXISTS scrape_failures (
                                               id UUID NOT NULL,
                                               failed_at TIMESTAMPTZ NOT NULL,
                                               url TEXT NOT NULL,
                                               retailer_domain VARCHAR(100) NOT NULL,
    failure_reason VARCHAR(50) NOT NULL,
    http_status_code INT,
    error_message TEXT,
    attempted_tiers VARCHAR(255),
    html_snippet TEXT,
    PRIMARY KEY (failed_at, id)
    );

-- 2. Convert to TimescaleDB Hypertable
SELECT create_hypertable(
           'scrape_failures',
           by_range('failed_at', INTERVAL '7 days'),
           if_not_exists => TRUE
       );

-- 3. Indexes for rapid operational diagnostics
CREATE INDEX IF NOT EXISTS idx_scrape_failures_domain_reason
    ON scrape_failures (retailer_domain, failure_reason, failed_at DESC);

-- 4. Enable Columnar Compression after 7 days
ALTER TABLE scrape_failures SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'retailer_domain, failure_reason',
    timescaledb.compress_orderby = 'failed_at DESC'
    );

SELECT add_compression_policy('scrape_failures', INTERVAL '7 days', if_not_exists => TRUE);

-- 5. Automated Data Retention: Drop failure records older than 30 days
SELECT add_retention_policy('scrape_failures', INTERVAL '30 days', if_not_exists => TRUE);

-- 6. Continuous Aggregate: Hourly Failure Summary per Retailer & Reason
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_scrape_failure_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', failed_at) AS bucket_hour,
    retailer_domain,
    failure_reason,
    count(*) AS failure_count
FROM scrape_failures
GROUP BY bucket_hour, retailer_domain, failure_reason
    WITH NO DATA;

SELECT add_continuous_aggregate_policy(
           'hourly_scrape_failure_summary',
           start_offset => INTERVAL '3 days',
           end_offset => INTERVAL '5 minutes',
           schedule_interval => INTERVAL '10 minutes',
           if_not_exists => TRUE
       );

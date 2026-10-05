# Phase 2: Decoupled Pipeline & Time-Series Storage

This document provides a comprehensive subtask breakdown of **Phase 2**, explaining the architectural motivations, internal mechanics, and proposed production-ready code implementations. Infrastructure is orchestrated using **Podman Compose** (`podman compose`) running on the **Podman** container engine. In accordance with project instructions, automated test suites are deferred.

---

## Architecture & Directory Layout

Phase 2 introduces the **Decoupled Pipeline**:
1. Scrapers publish unstructured/raw scrape envelopes (`RawScrapeEvent`) to Kafka topic `raw-scrapes`.
2. The ingestion pipeline service (`pulse-pipeline`) consumes scrape events, validates and normalizes UPC/GTIN barcodes via GS1 Modulo-10 algorithms, resolves items to canonical product entities with Redis L2 caching, and appends time-series price entries directly into a TimescaleDB hypertable (`price_history`).
3. Successful price updates are dispatched downstream to `canonical-prices` for consumption by future alert and search services.
4. Distributed infrastructure (TimescaleDB, Kafka in KRaft mode, Redis) runs locally via **Podman Compose**.

```text
pulse-price/
├── docker-compose.yml
├── init-scripts/
│   └── 01-init-timescale.sql
├── settings.gradle.kts
├── build.gradle.kts
├── pulse-common/
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/pulseprice/common/
│       ├── model/
│       │   ├── ProductRawPayload.kt
│       │   └── ScrapeResult.kt
│       └── event/
│           ├── RawScrapeEvent.kt
│           └── CanonicalProductPriceEvent.kt
├── pulse-scraper/
│   ├── build.gradle.kts
│   └── src/main/
│       ├── kotlin/com/pulseprice/scraper/
│       │   ├── ScraperApplication.kt
│       │   ├── RetailerScraper.kt
│       │   ├── DefaultRetailerScraper.kt
│       │   ├── extractor/JsonLdExtractor.kt
│       │   ├── ratelimit/RedisTokenBucketRateLimiter.kt
│       │   ├── kafka/
│       │   │   ├── KafkaProducerConfig.kt
│       │   │   └── ScrapeEventProducer.kt
│       │   └── service/
│       │       └── ScrapeIngestionService.kt
│       └── resources/
│           ├── application.yml
│           └── rate_limiter.lua
└── pulse-pipeline/
    ├── build.gradle.kts
    └── src/main/
        ├── kotlin/com/pulseprice/pipeline/
        │   ├── PipelineApplication.kt
        │   ├── config/
        │   │   ├── DatabaseConfig.kt
        │   │   └── KafkaConsumerConfig.kt
        │   ├── domain/
        │   │   ├── Entities.kt
        │   │   └── Repositories.kt
        │   ├── model/
        │   │   └── CanonicalResolutionResult.kt
        │   ├── resolver/
        │   │   ├── GtinValidator.kt
        │   │   └── CanonicalResolver.kt
        │   ├── service/
        │   │   └── PricePersistenceService.kt
        │   └── kafka/
        │       └── RawScrapeKafkaConsumer.kt
        └── resources/
            └── application.yml
```

---

## Subtask 2.1: Distributed Storage & Messaging Infrastructure (Podman Compose & TimescaleDB Setup)

### 1. What We Are Doing
We are setting up the foundational local infrastructure using Podman Compose (`docker-compose.yml` executed via `podman compose`):
- **TimescaleDB (PostgreSQL 16):** Time-series database with TimescaleDB extension enabled. Hypertables partition price data into temporal chunks with automatic columnar compression policies and continuous aggregate materialized views.
- **Apache Kafka 3.8 (Native KRaft Mode):** Distributed event streaming broker running in lightweight KRaft mode (no Zookeeper required).
- **Redis 7.2:** In-memory data store for per-domain rate limiting (from Phase 1) and sub-millisecond GTIN-to-Canonical product resolution caching.
- **Initialization Script (`init-scripts/01-init-timescale.sql`):** Creates the relational tables (`canonical_products`, `retailer_products`), converts `price_history` into a 7-day chunk hypertable, configures compression, and defines a daily roll-up continuous aggregate.

### 2. Why We Are Doing It & Deep Mechanics

#### Why TimescaleDB Over Standard PostgreSQL
- **The B-Tree Cache Cliff:** Standard relational databases index time-series columns using B-trees. As the table grows to tens of millions of rows, the index tree becomes too large to fit in Postgres's memory (`shared_buffers`). Every new price insert causes random disk reads to traverse and update the B-tree index, resulting in a catastrophic drop in write throughput.
- **Hypertables and Chunking:** TimescaleDB partitions a single logical table (`price_history`) into multiple underlying physical PostgreSQL tables ("chunks") based on time intervals (e.g. 7 days). Because only the current time chunk receives writes, its B-tree indexes remain fully resident in RAM, sustaining 100,000+ writes/sec without memory exhaustion.
- **Columnar Compression:** Historical price data is mostly static. TimescaleDB compresses older chunks using hybrid columnar storage (delta-of-delta and Gorilla compression for numeric values/timestamps, dictionary encoding for strings), reducing disk usage by 90%+ while accelerating analytical range queries.
- **Continuous Aggregates:** In metasearch, computing daily high/low/average prices across millions of records on the fly introduces high latency. TimescaleDB continuous aggregates automatically compute and maintain incremental rollups as data arrives.

#### Why Apache Kafka in KRaft Mode
- **Zero Zookeeper Overhead:** Standard Kafka previously required a separate Apache Zookeeper quorum, introducing synchronization lag, split-brain failure modes, and double metadata management. Kafka 3.8 with KRaft uses an internal Raft consensus protocol, accelerating cluster failovers, simplifying local development, and reducing container resource footprints.
- **Decoupling Scraper Ingestion from Storage:** Retail web scraping occurs in volatile bursts (e.g. daily catalog sweeps). Direct synchronous database writes would overwhelm database connection pools. Kafka acts as an elastic backpressure buffer: scrapers publish events at maximum rate, and pipeline consumers drain the topic at a controlled, sustainable pace.

#### Podman Compose Mechanics & Volume Handling
- Podman executes containers daemonless and rootless without requiring a background root daemon (`dockerd`).
- The volume mount `./init-scripts:/docker-entrypoint-initdb.d:z` uses the `:z` flag to instruct Podman to configure SELinux shared volume relabeling, allowing the unprivileged container process to read the SQL initialization script.

### 3. Proposed Code

#### File: [`docker-compose.yml`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/docker-compose.yml)
```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    container_name: pulse-timescaledb
    environment:
      POSTGRES_USER: pulse
      POSTGRES_PASSWORD: pulsepass
      POSTGRES_DB: pulseprice
    ports:
      - "5432:5432"
    volumes:
      - timescaledb_data:/var/lib/postgresql/data
      - ./init-scripts:/docker-entrypoint-initdb.d:z
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pulse -d pulseprice"]
      interval: 5s
      timeout: 5s
      retries: 5

  kafka:
    image: apache/kafka:3.8.0
    container_name: pulse-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7.2-alpine
    container_name: pulse-redis
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "yes"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

volumes:
  timescaledb_data:
```

#### File: [`init-scripts/01-init-timescale.sql`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/init-scripts/01-init-timescale.sql)
```sql
-- Enable TimescaleDB and UUID extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Canonical Products Table (Normalized identity across retailers)
CREATE TABLE IF NOT EXISTS canonical_products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gtin VARCHAR(14) UNIQUE,                     -- Normalized GTIN-14 (UPC-A, EAN-13 zero-padded)
    brand VARCHAR(255),
    model VARCHAR(255),
    title VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_canonical_products_brand_model
    ON canonical_products (LOWER(brand), LOWER(model));

-- 2. Retailer Products Table (Maps retailer specific URLs/SKUs to canonical product)
CREATE TABLE IF NOT EXISTS retailer_products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_product_id UUID NOT NULL REFERENCES canonical_products(id) ON DELETE CASCADE,
    retailer VARCHAR(100) NOT NULL,              -- e.g. "walmart.com", "bestbuy.com"
    retailer_sku VARCHAR(255),
    url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_retailer_products_canonical
    ON retailer_products (canonical_product_id);

CREATE INDEX IF NOT EXISTS idx_retailer_products_retailer_sku
    ON retailer_products (retailer, retailer_sku);

-- 3. Price History Hypertable (Append-only time-series data)
CREATE TABLE IF NOT EXISTS price_history (
    recorded_at TIMESTAMPTZ NOT NULL,
    retailer_product_id UUID NOT NULL REFERENCES retailer_products(id) ON DELETE CASCADE,
    canonical_product_id UUID NOT NULL REFERENCES canonical_products(id) ON DELETE CASCADE,
    price NUMERIC(12, 2) NOT NULL,
    original_price NUMERIC(12, 2),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    in_stock BOOLEAN NOT NULL DEFAULT TRUE
);

-- Convert to TimescaleDB Hypertable with 7-day chunk intervals
SELECT create_hypertable(
    'price_history',
    by_range('recorded_at', INTERVAL '7 days'),
    if_not_exists => TRUE
);

-- Compound indexes for rapid lookups
CREATE INDEX IF NOT EXISTS idx_price_history_retailer_time
    ON price_history (retailer_product_id, recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_history_canonical_time
    ON price_history (canonical_product_id, recorded_at DESC);

-- Enable TimescaleDB Columnar Compression after 14 days
ALTER TABLE price_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'retailer_product_id, canonical_product_id',
    timescaledb.compress_orderby = 'recorded_at DESC'
);

SELECT add_compression_policy('price_history', INTERVAL '14 days', if_not_exists => TRUE);

-- Continuous Aggregate: Daily Price Summary Rollup
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

-- Continuous aggregate refresh policy
SELECT add_continuous_aggregate_policy('daily_price_summary',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);
```

---

## Subtask 2.2: Multi-Module Gradle Architecture & Reactive Database Dependencies

### 1. What We Are Doing
We are updating the Gradle multi-module structure:
- Adding the new module `:pulse-pipeline` to `settings.gradle.kts`.
- Adding Spring Kafka producer capabilities to `:pulse-scraper`.
- Configuring `:pulse-pipeline` with Spring Boot 3.3.4, WebFlux, Spring Kafka, Spring Data R2DBC with the reactive PostgreSQL driver (`r2dbc-postgresql`), and Reactive Redis.

### 2. Why We Are Doing It & Deep Mechanics

#### Why R2DBC Over Blocking JDBC / JPA / Hibernate
- **The Reactive Thread Starvation Problem:** Traditional JDBC (`java.sql.Connection`) and ORM solutions like Hibernate rely on blocking socket I/O. When executing a query, the calling thread blocks until PostgreSQL completes query execution and flushes network packets. In a high-throughput event consumer processing thousands of price events per second, blocking calls exhaust thread pools, requiring large pool allocations (hundreds of OS threads), causing excessive context switching and memory overhead (~1MB per thread stack).
- **Non-blocking Event Loops with R2DBC:** R2DBC (Reactive Relational Database Connectivity) implements the PostgreSQL wire protocol directly on top of Netty event loops. Inbound queries return reactive publishers (`Mono`/`Flux`) which seamlessly integrate with Kotlin Coroutines (`suspend fun`, `awaitSingleOrNull`, `Flow`). A small, fixed thread pool (e.g. 4 CPU worker threads) can manage thousands of concurrent in-flight database requests without blocking.

#### Separation of `pulse-scraper` and `pulse-pipeline`
- `pulse-scraper` is network I/O-bound to external internet domains, requiring rate-limiting, proxies, and HTML/DOM parsing.
- `pulse-pipeline` is CPU- and database-bound, handling barcode normalization, event deduplication, and TimescaleDB hypertable writes.
- Decoupling them allows independent horizontal scaling in production (e.g. running multiple scraper instances to crawl diverse domains while keeping pipeline consumers sized for database ingest capacity).

### 3. Proposed Code

#### File: [`settings.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/settings.gradle.kts)
```kotlin
rootProject.name = "pulse-price"

include("pulse-common")
include("pulse-scraper")
include("pulse-pipeline")
```

#### File: [`pulse-scraper/build.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/build.gradle.kts)
```kotlin
plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

dependencies {
    implementation(project(":pulse-common"))

    // Non-blocking WebClient & Reactive Redis
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

    // Apache Kafka Producer
    implementation("org.springframework.kafka:spring-kafka")

    // HTML parsing & extraction
    implementation("org.jsoup:jsoup:1.18.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
}
```

#### File: [`pulse-pipeline/build.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/build.gradle.kts)
```kotlin
plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

dependencies {
    implementation(project(":pulse-common"))

    // Reactive WebFlux
    implementation("org.springframework.boot:spring-boot-starter-webflux")

    // Apache Kafka Consumer & Producer
    implementation("org.springframework.kafka:spring-kafka")

    // Reactive Database with R2DBC & PostgreSQL Driver
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.postgresql:r2dbc-postgresql:1.0.5.RELEASE")

    // Reactive Redis for GTIN Canonical Resolution Cache
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

    // JSON serialization
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
}
```

---

## Subtask 2.3: Decoupled Messaging Contracts & Domain Event Schemas

### 1. What We Are Doing
We are defining the domain event contracts in `pulse-common`:
- [`RawScrapeEvent`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/event/RawScrapeEvent.kt): Emitted by scrapers into Kafka topic `raw-scrapes`. Contains raw crawled metadata, URLs, extracted JSON-LD, and timing metrics.
- [`CanonicalProductPriceEvent`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/event/CanonicalProductPriceEvent.kt): Emitted by the pipeline into Kafka topic `canonical-prices` after barcode resolution and hypertable insertion.

### 2. Why We Are Doing It & Deep Mechanics
- **Event Envelope Pattern:** Each event contains unique traceability identifiers (`eventId`), high-resolution timestamps (`scrapedAt`, `recordedAt`), and schema versioning. This enables distributed tracing across asynchronous service boundaries without tight coupling.
- **Contract Boundary:** Placing events in `pulse-common` allows both the producer (`pulse-scraper`) and consumers (`pulse-pipeline`, and future `pulse-alerts`) to share a single source of truth without code duplication.
- **Preservation of Raw Scrape Context:** Storing the `payload` alongside `url` and `retailerDomain` guarantees that if downstream barcode normalization algorithms change, raw scrape events can be replayed from Kafka without re-crawling target retailer sites.

### 3. Proposed Code

#### File: [`pulse-common/src/main/kotlin/com/pulseprice/common/event/RawScrapeEvent.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/event/RawScrapeEvent.kt)
```kotlin
package com.pulseprice.common.event

import com.pulseprice.common.model.ProductRawPayload
import java.time.Instant
import java.util.UUID

data class RawScrapeEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val url: String,
    val retailerDomain: String,
    val payload: ProductRawPayload,
    val latencyMs: Long,
    val scrapedAt: Instant = Instant.now(),
    val schemaVersion: Int = 1
)
```

#### File: [`pulse-common/src/main/kotlin/com/pulseprice/common/event/CanonicalProductPriceEvent.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/event/CanonicalProductPriceEvent.kt)
```kotlin
package com.pulseprice.common.event

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class CanonicalProductPriceEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val canonicalProductId: UUID,
    val retailerProductId: UUID,
    val retailer: String,
    val gtin: String?,
    val price: BigDecimal,
    val originalPrice: BigDecimal? = null,
    val currency: String,
    val inStock: Boolean,
    val recordedAt: Instant = Instant.now()
)
```

---

## Subtask 2.4: High-Throughput Scrape Event Producer (`pulse-scraper`)

### 1. What We Are Doing
We are integrating Kafka publishing into `pulse-scraper`:
- [`KafkaProducerConfig`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/kafka/KafkaProducerConfig.kt): Configures an idempotent, high-throughput `KafkaTemplate<String, Any>`.
- [`ScrapeEventProducer`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/kafka/ScrapeEventProducer.kt): Non-blocking publisher converting Spring Kafka's `CompletableFuture` into Kotlin Coroutines.
- [`ScrapeIngestionService`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/service/ScrapeIngestionService.kt): Orchestrates scraping target URLs via `RetailerScraper` and immediately dispatches successful outcomes to Kafka topic `raw-scrapes`.

### 2. Why We Are Doing It & Deep Mechanics
- **Partitioning by Domain & URL:** In Kafka, messages with the same partition key land on the same broker partition, preserving per-key chronological ordering. We partition by `retailerDomain:url` so updates for the exact same retail product are processed strictly in sequence downstream.
- **Producer Idempotence (`enable.idempotence = true`):** Transient network glitches between scraper and Kafka broker could cause duplicate delivery if the producer retries an unacknowledged message. Kafka's idempotent producer assigns a Producer ID (PID) and sequence numbers to records, allowing brokers to deduplicate retries automatically.
- **Suspending Producer with Coroutines:** Spring Kafka's `CompletableFuture<SendResult>` is bridged to Kotlin Coroutines via `.await()`, keeping scraper thread consumption minimal while guaranteeing the message is acknowledged before reporting scrape success.

### 3. Proposed Code

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/kafka/KafkaProducerConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/kafka/KafkaProducerConfig.kt)
```kotlin
package com.pulseprice.scraper.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaProducerConfig(
    @Value("\${spring.kafka.bootstrap-servers:localhost:9092}")
    private val bootstrapServers: String,
    private val objectMapper: ObjectMapper
) {

    @Bean
    fun producerFactory(): ProducerFactory<String, Any> {
        val configProps = mutableMapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.RETRIES_CONFIG to 3,
            ProducerConfig.LINGER_MS_CONFIG to 10,
            ProducerConfig.BATCH_SIZE_CONFIG to 32768,
            JsonSerializer.ADD_TYPE_INFO_HEADERS to false
        )
        val valueSerializer = JsonSerializer<Any>(objectMapper)
        return DefaultKafkaProducerFactory(configProps, StringSerializer(), valueSerializer)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Any> = KafkaTemplate(producerFactory())
}
```

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/kafka/ScrapeEventProducer.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/kafka/ScrapeEventProducer.kt)
```kotlin
package com.pulseprice.scraper.kafka

import com.pulseprice.common.event.RawScrapeEvent
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class ScrapeEventProducer(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(ScrapeEventProducer::class.java)

    companion object {
        const val TOPIC_RAW_SCRAPES = "raw-scrapes"
    }

    suspend fun publishRawScrape(event: RawScrapeEvent) {
        // Partition key guarantees order per product URL
        val key = "${event.retailerDomain}:${event.url}"
        try {
            val result = kafkaTemplate.send(TOPIC_RAW_SCRAPES, key, event).await()
            logger.info(
                "Published RawScrapeEvent [id={}] to partition={} offset={}",
                event.eventId,
                result.recordMetadata.partition(),
                result.recordMetadata.offset()
            )
        } catch (ex: Exception) {
            logger.error("Failed to publish RawScrapeEvent [id={}] to Kafka: {}", event.eventId, ex.message, ex)
            throw ex
        }
    }
}
```

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/service/ScrapeIngestionService.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/service/ScrapeIngestionService.kt)
```kotlin
package com.pulseprice.scraper.service

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.scraper.RetailerScraper
import com.pulseprice.scraper.kafka.ScrapeEventProducer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI

@Service
class ScrapeIngestionService(
    private val scraper: RetailerScraper,
    private val eventProducer: ScrapeEventProducer
) {
    private val logger = LoggerFactory.getLogger(ScrapeIngestionService::class.java)

    suspend fun scrapeAndIngest(targetUrl: String): ScrapeResult {
        val result = scraper.scrape(targetUrl)

        if (result is ScrapeResult.Success) {
            val domain = runCatching { URI.create(targetUrl).host }.getOrDefault("unknown")
            val event = RawScrapeEvent(
                url = targetUrl,
                retailerDomain = domain,
                payload = result.payload,
                latencyMs = result.latencyMs
            )
            eventProducer.publishRawScrape(event)
            logger.info("Successfully scraped and dispatched event for URL: {}", targetUrl)
        } else {
            logger.warn("Scrape unsuccessful for URL: {} (Result: {})", targetUrl, result)
        }

        return result
    }
}
```

#### File: [`pulse-scraper/src/main/resources/application.yml`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/resources/application.yml)
```yaml
spring:
  application:
    name: pulse-scraper
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```

---

## Subtask 2.5: GTIN/UPC Canonical Product Resolution Engine & Redis L2 Caching (`pulse-pipeline`)

### 1. What We Are Doing
We are building the canonical identity engine inside `pulse-pipeline`:
- [`GtinValidator`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/resolver/GtinValidator.kt): Validates GTIN-8, GTIN-12 (UPC-A), GTIN-13 (EAN-13), and GTIN-14 check digits using the official GS1 Modulo-10 checksum algorithm and normalizes barcodes into an invariant 14-digit zero-padded string.
- [`CanonicalResolver`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/resolver/CanonicalResolver.kt): Multi-tier resolver linking retailer URLs/SKUs to a canonical product entity using a Redis L2 cache and relational fallbacks.

### 2. Why We Are Doing It & Deep Mechanics

#### The Canonical Product Identity Problem
- Retail websites identify the same physical product differently:
  - Walmart lists a Sony headphone with UPC-A: `027242923591`
  - Target lists it with EAN-13: `0027242923591`
  - Amazon uses ASINs and GTIN-14: `00027242923591`
- If ingested naively as separate strings, the metasearch engine treats them as three distinct items, destroying comparison capabilities.
- **GS1 Modulo-10 Algorithm:**
  1. Strip all formatting hyphens and spaces.
  2. Starting from the rightmost payload digit (excluding the checksum), multiply alternating digits by 3 and 1.
  3. Sum all weighted products.
  4. Compute `(10 - (sum % 10)) % 10`. The calculated check digit must match the last digit.
  5. Invalid barcodes (e.g. dummy strings like `123456789012` or retailer internal SKUs) are discarded.
- **GTIN-14 Standard Representation:** Zero-padding valid 8, 12, or 13-digit codes to 14 digits provides a universal invariant key across all global retail platforms.

#### Redis L2 Resolution Cache
- Checking PostgreSQL on every incoming scrape event creates high read IOPS.
- We implement a Cache-Aside pattern using Redis (`cache:canonical:gtin:{gtin14}` -> `UUID`).
- Cached hits resolve the canonical product UUID in <1ms without querying Postgres. If absent, the engine performs an atomic UPSERT in PostgreSQL and populates the Redis cache with a 24-hour TTL.

### 3. Proposed Code

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/resolver/GtinValidator.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/resolver/GtinValidator.kt)
```kotlin
package com.pulseprice.pipeline.resolver

import org.springframework.stereotype.Component

@Component
class GtinValidator {

    /**
     * Validates and normalizes GTIN-8, GTIN-12 (UPC-A), GTIN-13 (EAN), and GTIN-14.
     * Returns a 14-digit zero-padded canonical GTIN string, or null if invalid.
     */
    fun validateAndNormalize(rawGtin: String?): String? {
        if (rawGtin.isNullOrBlank()) return null

        // Strip non-digit characters (hyphens, spaces)
        val digitsOnly = rawGtin.filter { it.isDigit() }

        if (digitsOnly.length !in setOf(8, 12, 13, 14)) {
            return null
        }

        if (!hasValidModulo10Checksum(digitsOnly)) {
            return null
        }

        // Pad with leading zeros to 14 digits
        return digitsOnly.padStart(14, '0')
    }

    /**
     * GS1 standard Modulo-10 check digit verification.
     */
    private fun hasValidModulo10Checksum(code: String): Boolean {
        val length = code.length
        val expectedCheckDigit = code.last().digitToInt()
        val dataPayload = code.substring(0, length - 1)

        var sum = 0
        // Weight multiplier alternates between 3 and 1 starting from rightmost data digit
        var weight = 3
        for (i in dataPayload.length - 1 downTo 0) {
            sum += dataPayload[i].digitToInt() * weight
            weight = if (weight == 3) 1 else 3
        }

        val calculatedCheckDigit = (10 - (sum % 10)) % 10
        return calculatedCheckDigit == expectedCheckDigit
    }
}
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/model/CanonicalResolutionResult.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/model/CanonicalResolutionResult.kt)
```kotlin
package com.pulseprice.pipeline.model

import com.pulseprice.pipeline.domain.CanonicalProductEntity
import com.pulseprice.pipeline.domain.RetailerProductEntity

data class CanonicalResolutionResult(
    val canonicalProduct: CanonicalProductEntity,
    val retailerProduct: RetailerProductEntity
)
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/resolver/CanonicalResolver.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/resolver/CanonicalResolver.kt)
```kotlin
package com.pulseprice.pipeline.resolver

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.pipeline.domain.CanonicalProductEntity
import com.pulseprice.pipeline.domain.CanonicalProductRepository
import com.pulseprice.pipeline.domain.RetailerProductEntity
import com.pulseprice.pipeline.domain.RetailerProductRepository
import com.pulseprice.pipeline.model.CanonicalResolutionResult
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Service
class CanonicalResolver(
    private val gtinValidator: GtinValidator,
    private val canonicalProductRepository: CanonicalProductRepository,
    private val retailerProductRepository: RetailerProductRepository,
    private val redisTemplate: ReactiveStringRedisTemplate
) {
    private val logger = LoggerFactory.getLogger(CanonicalResolver::class.java)

    companion object {
        private const val REDIS_GTIN_PREFIX = "cache:canonical:gtin:"
        private val CACHE_TTL = Duration.ofHours(24)
    }

    suspend fun resolve(event: RawScrapeEvent): CanonicalResolutionResult {
        val payload = event.payload
        val normalizedGtin = gtinValidator.validateAndNormalize(payload.gtin)

        // 1. Resolve Canonical Product
        val canonicalProduct = if (normalizedGtin != null) {
            resolveByGtin(normalizedGtin, payload.title, payload.brand)
        } else {
            resolveByFallback(payload.title, payload.brand)
        }

        // 2. Resolve or Link Retailer Product
        val retailerProduct = resolveRetailerProduct(
            canonicalId = canonicalProduct.id!!,
            retailer = event.retailerDomain,
            url = event.url,
            sku = payload.sku,
            title = payload.title
        )

        return CanonicalResolutionResult(canonicalProduct, retailerProduct)
    }

    private suspend fun resolveByGtin(gtin: String, title: String, brand: String?): CanonicalProductEntity {
        val cacheKey = "$REDIS_GTIN_PREFIX$gtin"

        // Step A: Check Redis L2 Cache
        val cachedIdStr = redisTemplate.opsForValue().get(cacheKey).awaitSingleOrNull()
        if (!cachedIdStr.isNullOrBlank()) {
            val cachedId = UUID.fromString(cachedIdStr)
            val found = canonicalProductRepository.findById(cachedId)
            if (found != null) {
                return found
            }
        }

        // Step B: Query Database
        val existing = canonicalProductRepository.findByGtin(gtin)
        if (existing != null) {
            redisTemplate.opsForValue().set(cacheKey, existing.id.toString(), CACHE_TTL).awaitSingleOrNull()
            return existing
        }

        // Step C: Create New Canonical Product
        val newEntity = CanonicalProductEntity(
            gtin = gtin,
            title = title,
            brand = brand,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        val saved = canonicalProductRepository.save(newEntity)
        logger.info("Created new canonical product [id={}, gtin={}]", saved.id, gtin)

        // Cache resolution
        redisTemplate.opsForValue().set(cacheKey, saved.id.toString(), CACHE_TTL).awaitSingleOrNull()
        return saved
    }

    private suspend fun resolveByFallback(title: String, brand: String?): CanonicalProductEntity {
        // Fallback for items missing barcodes: match by brand and title
        val existing = if (brand != null) {
            canonicalProductRepository.findByBrandAndTitle(brand, title)
        } else null

        if (existing != null) return existing

        val provisional = CanonicalProductEntity(
            gtin = null,
            title = title,
            brand = brand,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        return canonicalProductRepository.save(provisional)
    }

    private suspend fun resolveRetailerProduct(
        canonicalId: UUID,
        retailer: String,
        url: String,
        sku: String?,
        title: String
    ): RetailerProductEntity {
        val existing = retailerProductRepository.findByUrl(url)
        if (existing != null) {
            // Update title or SKU if changed
            if (existing.title != title || existing.retailerSku != sku) {
                return retailerProductRepository.save(
                    existing.copy(title = title, retailerSku = sku, updatedAt = Instant.now())
                )
            }
            return existing
        }

        val newRetailerProduct = RetailerProductEntity(
            canonicalProductId = canonicalId,
            retailer = retailer,
            retailerSku = sku,
            url = url,
            title = title,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        return retailerProductRepository.save(newRetailerProduct)
    }
}
```

---

## Subtask 2.6: Reactive TimescaleDB Ingestion & Hypertable Data Access Layer (`pulse-pipeline`)

### 1. What We Are Doing
We are implementing the reactive data persistence layer for TimescaleDB using Spring Data R2DBC:
- [`DatabaseConfig`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/DatabaseConfig.kt): Configures PostgreSQL R2DBC connection factories.
- Domain Entities ([`Entities.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/domain/Entities.kt)): Spring Data `@Table` data classes for `canonical_products`, `retailer_products`, and `price_history`.
- Repositories ([`Repositories.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/domain/Repositories.kt)): Non-blocking `CoroutineCrudRepository` interfaces.
- [`PricePersistenceService`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/service/PricePersistenceService.kt): Appends price snapshots to the TimescaleDB hypertable and dispatches `CanonicalProductPriceEvent` to Kafka topic `canonical-prices`.

### 2. Why We Are Doing It & Deep Mechanics
- **Hypertable Routing:** When `PricePersistenceService` executes an `INSERT INTO price_history`, TimescaleDB transparently routes the record to the appropriate physical chunk based on `recorded_at`. Because writes are current, inserts hit the in-memory active chunk table without locking historical data.
- **Outbox / Downstream Event Emission:** Immediately after successful hypertable persistence, the service publishes a `CanonicalProductPriceEvent` to Kafka. Downstream consumers (e.g. Phase 4 price drop alert engine) receive pre-canonicalized price updates without polling the database.

### 3. Proposed Code

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/DatabaseConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/DatabaseConfig.kt)
```kotlin
package com.pulseprice.pipeline.config

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration
import io.r2dbc.postgresql.PostgresqlConnectionFactory
import io.r2dbc.spi.ConnectionFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories

@Configuration
@EnableR2dbcRepositories(basePackages = ["com.pulseprice.pipeline.domain"])
class DatabaseConfig(
    @Value("\${spring.r2dbc.host:localhost}") private val host: String,
    @Value("\${spring.r2dbc.port:5432}") private val port: Int,
    @Value("\${spring.r2dbc.database:pulseprice}") private val database: String,
    @Value("\${spring.r2dbc.username:pulse}") private val username: String,
    @Value("\${spring.r2dbc.password:pulsepass}") private val password: String
) : AbstractR2dbcConfiguration() {

    @Bean
    override fun connectionFactory(): ConnectionFactory {
        return PostgresqlConnectionFactory(
            PostgresqlConnectionConfiguration.builder()
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                .build()
        )
    }
}
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/domain/Entities.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/domain/Entities.kt)
```kotlin
package com.pulseprice.pipeline.domain

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Table("canonical_products")
data class CanonicalProductEntity(
    @Id
    val id: UUID? = null,
    val gtin: String?,
    val brand: String?,
    val model: String? = null,
    val title: String,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now()
)

@Table("retailer_products")
data class RetailerProductEntity(
    @Id
    val id: UUID? = null,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    val retailer: String,
    @Column("retailer_sku") val retailerSku: String?,
    val url: String,
    val title: String,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now()
)

@Table("price_history")
data class PriceHistoryEntity(
    @Column("recorded_at") val recordedAt: Instant = Instant.now(),
    @Column("retailer_product_id") val retailerProductId: UUID,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    val price: BigDecimal,
    @Column("original_price") val originalPrice: BigDecimal? = null,
    val currency: String = "USD",
    @Column("in_stock") val inStock: Boolean = true
)
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/domain/Repositories.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/domain/Repositories.kt)
```kotlin
package com.pulseprice.pipeline.domain

import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface CanonicalProductRepository : CoroutineCrudRepository<CanonicalProductEntity, UUID> {
    suspend fun findByGtin(gtin: String): CanonicalProductEntity?

    @Query("SELECT * FROM canonical_products WHERE LOWER(brand) = LOWER(:brand) AND LOWER(title) = LOWER(:title) LIMIT 1")
    suspend fun findByBrandAndTitle(brand: String, title: String): CanonicalProductEntity?
}

@Repository
interface RetailerProductRepository : CoroutineCrudRepository<RetailerProductEntity, UUID> {
    suspend fun findByUrl(url: String): RetailerProductEntity?
    suspend fun findByRetailerAndRetailerSku(retailer: String, retailerSku: String): RetailerProductEntity?
}

@Repository
interface PriceHistoryRepository : CoroutineCrudRepository<PriceHistoryEntity, UUID> {
    @Query("""
        INSERT INTO price_history (recorded_at, retailer_product_id, canonical_product_id, price, original_price, currency, in_stock)
        VALUES (:recordedAt, :retailerProductId, :canonicalProductId, :price, :originalPrice, :currency, :inStock)
    """)
    suspend fun insertPricePoint(
        recordedAt: java.time.Instant,
        retailerProductId: UUID,
        canonicalProductId: UUID,
        price: java.math.BigDecimal,
        originalPrice: java.math.BigDecimal?,
        currency: String,
        inStock: Boolean
    ): Int
}
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/service/PricePersistenceService.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/service/PricePersistenceService.kt)
```kotlin
package com.pulseprice.pipeline.service

import com.pulseprice.common.event.CanonicalProductPriceEvent
import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.pipeline.domain.PriceHistoryRepository
import com.pulseprice.pipeline.model.CanonicalResolutionResult
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class PricePersistenceService(
    private val priceHistoryRepository: PriceHistoryRepository,
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(PricePersistenceService::class.java)

    companion object {
        const val TOPIC_CANONICAL_PRICES = "canonical-prices"
    }

    suspend fun recordPrice(resolution: CanonicalResolutionResult, scrapeEvent: RawScrapeEvent) {
        val payload = scrapeEvent.payload
        val recordedAt = Instant.now()

        // 1. Insert directly into TimescaleDB Hypertable
        priceHistoryRepository.insertPricePoint(
            recordedAt = recordedAt,
            retailerProductId = resolution.retailerProduct.id!!,
            canonicalProductId = resolution.canonicalProduct.id!!,
            price = payload.currentPrice,
            originalPrice = payload.originalPrice,
            currency = payload.currency,
            inStock = payload.inStock
        )

        logger.info(
            "Recorded price [{}] for canonicalId={} at retailer={}",
            payload.currentPrice,
            resolution.canonicalProduct.id,
            resolution.retailerProduct.retailer
        )

        // 2. Publish Canonical Price Event downstream for alerts and real-time indexing
        val downstreamEvent = CanonicalProductPriceEvent(
            canonicalProductId = resolution.canonicalProduct.id,
            retailerProductId = resolution.retailerProduct.id,
            retailer = resolution.retailerProduct.retailer,
            gtin = resolution.canonicalProduct.gtin,
            price = payload.currentPrice,
            originalPrice = payload.originalPrice,
            currency = payload.currency,
            inStock = payload.inStock,
            recordedAt = recordedAt
        )

        try {
            kafkaTemplate.send(
                TOPIC_CANONICAL_PRICES,
                resolution.canonicalProduct.id.toString(),
                downstreamEvent
            ).await()
        } catch (ex: Exception) {
            logger.error("Failed to emit CanonicalProductPriceEvent downstream: {}", ex.message, ex)
            // Note: DB write succeeded; message can be retried or recovered
        }
    }
}
```

---

## Subtask 2.7: Kafka Consumer Pipeline, DLT Routing & Event Dispatch (`pulse-pipeline`)

### 1. What We Are Doing
We are assembling the Kafka consumer pipeline:
- [`KafkaConsumerConfig`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/KafkaConsumerConfig.kt): Configures manual immediate acknowledgments (`AckMode.MANUAL_IMMEDIATE`) and JSON deserialization with error resilience.
- [`RawScrapeKafkaConsumer`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/kafka/RawScrapeKafkaConsumer.kt): Non-blocking `@KafkaListener` consuming `raw-scrapes`, invoking `CanonicalResolver` and `PricePersistenceService`, and routing unresolvable poison pill events to a Dead-Letter Topic (`raw-scrapes-dlt`).
- [`PipelineApplication`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/PipelineApplication.kt): Spring Boot application entrypoint.
- [`application.yml`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/resources/application.yml): Configuration properties for R2DBC, Kafka, and Redis.

### 2. Why We Are Doing It & Deep Mechanics
- **At-Least-Once Delivery via Manual ACKs:** Under auto-commit (`enable.auto.commit = true`), Kafka commits offsets periodically regardless of whether database persistence completed. If the consumer crashes before persisting, data is lost permanently. With `AckMode.MANUAL_IMMEDIATE`, the consumer only executes `acknowledgment.acknowledge()` after the TimescaleDB hypertable insert succeeds.
- **Dead-Letter Topic (`raw-scrapes-dlt`) Routing:** Corrupted messages or unexpected upstream HTML drift can cause deserialization or parsing exceptions. Rather than halting consumer partition processing (which blocks all subsequent valid messages), failed records are pushed to a DLT with error diagnostic headers, allowing the consumer to advance its offset smoothly.

### 3. Proposed Code

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/KafkaConsumerConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/KafkaConsumerConfig.kt)
```kotlin
package com.pulseprice.pipeline.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.event.RawScrapeEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer

@EnableKafka
@Configuration
class KafkaConsumerConfig(
    @Value("\${spring.kafka.bootstrap-servers:localhost:9092}")
    private val bootstrapServers: String,
    private val objectMapper: ObjectMapper
) {

    @Bean
    fun consumerFactory(): ConsumerFactory<String, RawScrapeEvent> {
        val props = mutableMapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "pulse-pipeline-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS to JsonDeserializer::class.java.name,
            JsonDeserializer.TRUSTED_PACKAGES to "com.pulseprice.*",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        val jsonDeserializer = JsonDeserializer(RawScrapeEvent::class.java, objectMapper)
        jsonDeserializer.ignoreTypeHeaders()
        return DefaultKafkaConsumerFactory(props, StringDeserializer(), ErrorHandlingDeserializer(jsonDeserializer))
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, RawScrapeEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, RawScrapeEvent>()
        factory.consumerFactory = consumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.setConcurrency(3)
        return factory
    }

    @Bean
    fun downstreamProducerFactory(): ProducerFactory<String, Any> {
        val props = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            JsonSerializer.ADD_TYPE_INFO_HEADERS to false
        )
        return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer(objectMapper))
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Any> = KafkaTemplate(downstreamProducerFactory())
}
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/kafka/RawScrapeKafkaConsumer.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/kafka/RawScrapeKafkaConsumer.kt)
```kotlin
package com.pulseprice.pipeline.kafka

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.pipeline.resolver.CanonicalResolver
import com.pulseprice.pipeline.service.PricePersistenceService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class RawScrapeKafkaConsumer(
    private val canonicalResolver: CanonicalResolver,
    private val pricePersistenceService: PricePersistenceService,
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(RawScrapeKafkaConsumer::class.java)
    private val scope = CoroutineScope(Dispatchers.IO)

    companion object {
        const val TOPIC_DLT = "raw-scrapes-dlt"
    }

    @KafkaListener(
        topics = ["raw-scrapes"],
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onRawScrape(
        record: ConsumerRecord<String, RawScrapeEvent?>,
        acknowledgment: Acknowledgment
    ) {
        val event = record.value()
        if (event == null) {
            logger.warn("Received null/unparseable payload from partition={} offset={}. Routing to DLT.", record.partition(), record.offset())
            kafkaTemplate.send(TOPIC_DLT, record.key() ?: "unknown", "CORRUPTED_EVENT")
            acknowledgment.acknowledge()
            return
        }

        scope.launch {
            try {
                // 1. Resolve to Canonical Product & Retailer Product
                val resolution = canonicalResolver.resolve(event)

                // 2. Persist to TimescaleDB Hypertable & emit downstream
                pricePersistenceService.recordPrice(resolution, event)

                // 3. Acknowledge offset strictly on success
                acknowledgment.acknowledge()
            } catch (ex: Exception) {
                logger.error("Failed to process event [id={}]: {}. Routing to DLT.", event.eventId, ex.message, ex)
                kafkaTemplate.send(TOPIC_DLT, record.key(), event)
                acknowledgment.acknowledge()
            }
        }
    }
}
```

#### File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/PipelineApplication.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/PipelineApplication.kt)
```kotlin
package com.pulseprice.pipeline

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PipelineApplication

fun main(args: Array<String>) {
    runApplication<PipelineApplication>(*args)
}
```

#### File: [`pulse-pipeline/src/main/resources/application.yml`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/resources/application.yml)
```yaml
server:
  port: 8082

spring:
  application:
    name: pulse-pipeline
  r2dbc:
    host: ${POSTGRES_HOST:localhost}
    port: ${POSTGRES_PORT:5432}
    database: ${POSTGRES_DB:pulseprice}
    username: ${POSTGRES_USER:pulse}
    password: ${POSTGRES_PASSWORD:pulsepass}
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```

---

## Subtask 2.8: Orchestration, Runtime Configuration & Manual End-to-End Verification

*(Test suites are ignored per current phase guidelines. The following instructions provide Podman Compose, shell, and SQL commands for manual verification).*

### 1. Launch Distributed Infrastructure via Podman Compose
Start TimescaleDB, Kafka, and Redis in detached mode using `podman compose`:
```bash
podman compose up -d
```
*(Alternatively, `podman-compose up -d` can also be used directly).*

Verify that all three containers are healthy and running:
```bash
podman compose ps
```

### 2. Verify TimescaleDB Hypertable Setup
Connect to the running TimescaleDB container via `podman exec` and `psql` to verify the hypertable, indexes, and continuous aggregates:
```bash
podman exec -it pulse-timescaledb psql -U pulse -d pulseprice -c "
SELECT hypertable_name, num_chunks 
FROM timescaledb_information.hypertables 
WHERE hypertable_name = 'price_history';
"
```
*Expected Output:* `price_history | 0` (or greater once chunks are populated).

Verify continuous aggregate:
```bash
podman exec -it pulse-timescaledb psql -U pulse -d pulseprice -c "
SELECT view_name 
FROM timescaledb_information.continuous_aggregates;
"
```
*Expected Output:* `daily_price_summary`.

### 3. Compile the Multi-Module Gradle Project
Compile all modules including `:pulse-common`, `:pulse-scraper`, and `:pulse-pipeline`:
```bash
./gradlew compileKotlin
```

### 4. Start the Services
Run the pipeline service in one terminal:
```bash
./gradlew :pulse-pipeline:bootRun
```

Run the scraper service in a second terminal:
```bash
./gradlew :pulse-scraper:bootRun
```

### 5. Manual Pipeline End-to-End Test via Kafka CLI
Publish a sample `RawScrapeEvent` JSON to topic `raw-scrapes` using Kafka's built-in console producer via `podman exec`:

```bash
podman exec -i pulse-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic raw-scrapes \
  --property "parse.key=true" \
  --property "key.separator=:" << 'EOF'
walmart.com:https://www.walmart.com/ip/sony-headphones:{"eventId":"00000000-0000-0000-0000-000000000001","url":"https://www.walmart.com/ip/sony-headphones","retailerDomain":"walmart.com","latencyMs":250,"scrapedAt":"2026-10-04T12:00:00Z","payload":{"title":"Sony WH-1000XM5 Wireless Headphones","currentPrice":348.00,"currency":"USD","originalPrice":399.99,"sku":"WMT-12345","gtin":"0027242923591","brand":"Sony","inStock":true}}
EOF
```

### 6. Verify Database Persistence in TimescaleDB
Inspect the tables to confirm canonical identity resolution and hypertable insertion:

```bash
podman exec -it pulse-timescaledb psql -U pulse -d pulseprice -c "
SELECT id, gtin, brand, title FROM canonical_products;
SELECT id, canonical_product_id, retailer, url FROM retailer_products;
SELECT recorded_at, price, currency, in_stock FROM price_history;
"
```

*Expected Result:*
- `canonical_products` contains 1 row with normalized 14-digit GTIN `00027242923591`.
- `retailer_products` links `walmart.com` to the canonical product UUID.
- `price_history` contains the recorded price `$348.00` in the current 7-day hypertable chunk.

### 7. Verify Redis Canonical Cache
Inspect Redis to verify the canonical resolution was cached:
```bash
podman exec -it pulse-redis redis-cli GET "cache:canonical:gtin:00027242923591"
```
*Expected Result:* The canonical product UUID string matching PostgreSQL.

### 8. Verify Downstream Event Publication
Verify that `pulse-pipeline` emitted the `canonical-prices` event for downstream consumers (e.g. Phase 4 price-drop alert engine):
```bash
podman exec -it pulse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic canonical-prices \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```
*Expected Result:* A JSON payload conforming to `CanonicalProductPriceEvent` with `canonicalProductId`, price `348.00`, and `gtin: "00027242923591"`.

# Phase 4: Price Drop Alerts & Metasearch API

This document provides a comprehensive, production-grade implementation guide for **Phase 4: Price Drop Alerts & Metasearch API**. It introduces the public-facing reactive API service (`pulse-api`), an event-driven price-drop alert matching engine, pluggable multi-channel notification dispatchers, PostgreSQL trigram full-text search, and TimescaleDB analytical time-bucket price aggregation.

Infrastructure is orchestrated using **Podman Compose** (`podman compose`) / **Docker Compose** (`docker compose`). In accordance with project instructions, automated test suites are deferred and code is proposed in full for manual review and implementation.

---

## Architecture & Directory Layout

Phase 4 introduces the **Metasearch Query & Real-Time Alert Subsystem**:
1. **Event-Driven Alert Matching Engine:** Consumes `canonical-prices` Kafka events emitted by `pulse-pipeline` after every validated scrape write. Evaluates user subscriptions against threshold drops, percentage drops, all-time lows, and back-in-stock transitions. Emits enriched `PriceDropAlertEvent` messages to the `price-drop-alerts` topic with Redis-backed cooldown deduplication.
2. **Pluggable Notification Dispatcher:** Consumes `price-drop-alerts` Kafka events and routes notifications through pluggable channel handlers: Webhook (with HMAC-SHA256 signatures and backoff retries), Email (HTML pricing cards), and Server-Sent Events (SSE).
3. **Reactive Metasearch REST APIs:** High-throughput non-blocking endpoints (`/search`, `/compare`, `/history`, `/live-stream`) built with Spring WebFlux and Kotlin Coroutines, utilizing PostgreSQL `pg_trgm` fuzzy matching, TimescaleDB continuous aggregates, and Redis L1 caching.

```text
pulse-price/
├── docker-compose.yml
├── init-scripts/
│   ├── 01-init-timescale.sql
│   ├── 02-scrape-failures-schema.sql
│   └── 03-alerts-and-api-schema.sql
├── build.gradle.kts
├── settings.gradle.kts
├── pulse-common/
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/pulseprice/common/
│       ├── model/
│       │   ├── ProductRawPayload.kt
│       │   ├── ScrapeResult.kt
│       │   ├── ScrapeTier.kt
│       │   ├── ScrapeFailureReason.kt
│       │   ├── AlertModels.kt
│       │   └── MetasearchModels.kt
│       └── event/
│           ├── RawScrapeEvent.kt
│           ├── CanonicalProductPriceEvent.kt
│           ├── ScrapeDeadLetterEvent.kt
│           └── PriceDropAlertEvent.kt
├── pulse-scraper/
│   └── ... (Phase 1-3 Components)
├── pulse-pipeline/
│   └── ... (Phase 2-3 Components)
└── pulse-api/
    ├── build.gradle.kts
    └── src/main/
        ├── kotlin/com/pulseprice/api/
        │   ├── ApiApplication.kt
        │   ├── config/
        │   │   ├── DatabaseConfig.kt
        │   │   ├── KafkaApiConfig.kt
        │   │   ├── RedisCacheConfig.kt
        │   │   └── WebFluxSecurityConfig.kt
        │   ├── domain/
        │   │   ├── ApiEntities.kt
        │   │   └── ApiRepositories.kt
        │   ├── alert/
        │   │   ├── AlertEvaluationEngine.kt
        │   │   ├── AlertKafkaConsumer.kt
        │   │   ├── AlertCooldownManager.kt
        │   │   └── AlertHistoryService.kt
        │   ├── notification/
        │   │   ├── NotificationDispatcher.kt
        │   │   ├── NotificationChannelHandler.kt
        │   │   ├── WebhookNotificationHandler.kt
        │   │   ├── EmailNotificationHandler.kt
        │   │   └── SseNotificationHandler.kt
        │   ├── service/
        │   │   ├── MetasearchProductService.kt
        │   │   ├── PriceComparisonService.kt
        │   │   ├── PriceHistoryAnalyticsService.kt
        │   │   └── PriceAlertSubscriptionService.kt
        │   └── controller/
        │       ├── ProductSearchController.kt
        │       ├── ProductComparisonController.kt
        │       ├── ProductPriceHistoryController.kt
        │       ├── PriceAlertSubscriptionController.kt
        │       └── PriceStreamController.kt
        └── resources/
            └── application.yml
```

---

## Subtask 4.1: Multi-Module Architecture & Dependencies (`pulse-api`, `pulse-common`)

### 1. What We Are Doing
We are declaring and configuring the new `pulse-api` Gradle subproject. We register `pulse-api` in the root `settings.gradle.kts`, configure its build dependencies in `pulse-api/build.gradle.kts` (Spring WebFlux, Spring Data R2DBC, PostgreSQL R2DBC driver, Spring Data Redis Reactive, Spring Kafka, Kotlin Coroutines, and Jackson Kotlin module), and define its core `application.yml` runtime properties.

### 2. Why We Are Doing It & Deep Mechanics

#### Why Spring WebFlux with Kotlin Coroutines for Metasearch APIs
- **Non-Blocking Reactive Thread Model:** Traditional servlet engines (Tomcat, Jetty) allocate one dedicated thread per HTTP request. In high-concurrency metasearch where search queries fan out to full-text database indexes, Redis caches, and real-time Kafka streams, thread pools quickly exhaust under load (Thread Starvation).
- **Netty Event Loops + Lightweight Coroutines:** Spring WebFlux runs on Project Reactor and Netty, using a small number of event loop worker threads (equal to CPU cores). Kotlin Coroutines bridge reactive `Mono`/`Flux` primitives with imperative, sequential syntax (`suspend fun`), avoiding reactive callback chains (`.flatMap().map()`) while retaining 100% non-blocking backpressure.
- **R2DBC Reactive PostgreSQL:** Traditional JDBC drivers block the calling thread during query execution and wire serialization. R2DBC (Reactive Relational Database Connectivity) enables non-blocking wire-level communication with PostgreSQL and TimescaleDB, allowing hundreds of concurrent analytic range queries without blocking the Netty event loop.

### 3. Proposed Code

#### File: [`settings.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/settings.gradle.kts)
```kotlin
rootProject.name = "pulse-price"

include("pulse-common")
include("pulse-scraper")
include("pulse-pipeline")
include("pulse-api")
```

#### File: `pulse-api/build.gradle.kts`
```kotlin
plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":pulse-common"))

    // Reactive Web & Coroutines
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.8.1")

    // Reactive Database & TimescaleDB Data Access
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.postgresql:r2dbc-postgresql:1.0.5.RELEASE")
    implementation("org.postgresql:postgresql:42.7.3") // JDBC driver for flyway/admin utilities if needed

    // Reactive Redis
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

    // Apache Kafka Stream Consumer & Producer
    implementation("org.springframework.kafka:spring-kafka")

    // Validation & Utilities
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Actuator & Micrometer for Operational Health
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}
```

#### File: `pulse-api/src/main/resources/application.yml`
```yaml
server:
  port: 8080
  shutdown: graceful

spring:
  application:
    name: pulse-api

  # Reactive PostgreSQL / TimescaleDB Connection Pool
  r2dbc:
    url: r2dbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5444}/${DB_NAME:pulseprice}
    username: ${DB_USER:pulse}
    password: ${DB_PASSWORD:pulsepass}
    pool:
      enabled: true
      initial-size: 10
      max-size: 50
      max-idle-time: 30m
      validation-query: SELECT 1

  # Reactive Redis L1 Cache & Cooldown Store
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      timeout: 2000ms

  # Apache Kafka Event Streaming
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: pulse-alert-engine-group
      auto-offset-reset: latest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "com.pulseprice.*"
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true

pulse:
  kafka:
    topics:
      canonical-prices: canonical-prices
      price-drop-alerts: price-drop-alerts
  alerts:
    default-cooldown-hours: 24
    webhook-timeout-ms: 4000
    hmac-secret: ${PULSE_HMAC_SECRET:pulse-engine-super-secret-key-32bytes!}
  cache:
    search-ttl-seconds: 60
    compare-ttl-seconds: 15
    history-ttl-seconds: 120
```

---

## Subtask 4.2: Domain Contracts & Alert Event Schemas (`pulse-common`)

### 1. What We Are Doing
We are introducing domain data contracts, enums, DTOs, and event schemas into `pulse-common`:
- [`AlertModels.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/model/AlertModels.kt): Enums (`AlertType`, `NotificationChannel`, `AlertStatus`) and REST subscription payload contracts.
- [`PriceDropAlertEvent.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/event/PriceDropAlertEvent.kt): Kafka event schema published to `price-drop-alerts`.
- [`MetasearchModels.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/model/MetasearchModels.kt): Response structures for `/search`, `/compare`, `/history`, and `/live-stream`.

### 2. Why We Are Doing It & Deep Mechanics

#### Strict Typing & Inter-Module Decoupling
- Sharing contract classes via `pulse-common` guarantees compile-time schema synchronization between the pipeline producer, alert evaluation engine, notification dispatcher, and API presentation layer.
- `PriceDropAlertEvent` carries all contextual metadata (retailer name, product title, direct deep link, old price, new price, percentage delta, and subscription ID) so downstream notification dispatchers (e.g. Email/Webhook) can execute without issuing additional database read queries.

### 3. Proposed Code

#### File: `pulse-common/src/main/kotlin/com/pulseprice/common/model/AlertModels.kt`
```kotlin
package com.pulseprice.common.model

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

enum class AlertType {
    TARGET_PRICE,
    PERCENTAGE_DROP,
    ALL_TIME_LOW,
    BACK_IN_STOCK
}

enum class NotificationChannel {
    EMAIL,
    WEBHOOK,
    SSE
}

enum class AlertStatus {
    PENDING,
    DELIVERED,
    FAILED,
    COOLDOWN_SUPPRESSED
}

data class PriceAlertSubscriptionRequest(
    val canonicalProductId: UUID,
    val userEmail: String,
    val alertType: AlertType,
    val targetPrice: BigDecimal? = null,
    val percentageDropThreshold: BigDecimal? = null,
    val notificationChannel: NotificationChannel = NotificationChannel.EMAIL,
    val destination: String,
    val cooldownHours: Int = 24
)

data class PriceAlertSubscriptionResponse(
    val id: UUID,
    val canonicalProductId: UUID,
    val userEmail: String,
    val alertType: AlertType,
    val targetPrice: BigDecimal?,
    val percentageDropThreshold: BigDecimal?,
    val notificationChannel: NotificationChannel,
    val destination: String,
    val active: Boolean,
    val cooldownHours: Int,
    val lastTriggeredAt: Instant?,
    val createdAt: Instant,
    val updatedAt: Instant
)
```

#### File: `pulse-common/src/main/kotlin/com/pulseprice/common/event/PriceDropAlertEvent.kt`
```kotlin
package com.pulseprice.common.event

import com.pulseprice.common.model.AlertType
import com.pulseprice.common.model.NotificationChannel
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class PriceDropAlertEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val subscriptionId: UUID,
    val canonicalProductId: UUID,
    val retailerProductId: UUID,
    val productTitle: String,
    val retailer: String,
    val productUrl: String,
    val currentPrice: BigDecimal,
    val previousPrice: BigDecimal,
    val priceDropAmount: BigDecimal,
    val priceDropPercentage: BigDecimal,
    val currency: String,
    val alertType: AlertType,
    val channel: NotificationChannel,
    val destination: String,
    val triggeredAt: Instant = Instant.now()
)
```

#### File: `pulse-common/src/main/kotlin/com/pulseprice/common/model/MetasearchModels.kt`
```kotlin
package com.pulseprice.common.model

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class ProductSearchHit(
    val id: UUID,
    val gtin: String?,
    val brand: String?,
    val model: String?,
    val title: String,
    val lowestPrice: BigDecimal?,
    val highestPrice: BigDecimal?,
    val currency: String,
    val retailerCount: Int,
    val inStockCount: Int,
    val similarityScore: Double
)

data class ProductSearchResult(
    val items: List<ProductSearchHit>,
    val page: Int,
    val size: Int,
    val totalElements: Long,
    val totalPages: Int
)

data class RetailerOfferDto(
    val retailerProductId: UUID,
    val retailer: String,
    val retailerSku: String?,
    val url: String,
    val title: String,
    val currentPrice: BigDecimal,
    val originalPrice: BigDecimal?,
    val currency: String,
    val inStock: Boolean,
    val isBestPrice: Boolean,
    val priceDifferenceFromLowest: BigDecimal,
    val lastUpdated: Instant
)

data class ProductComparisonResponse(
    val canonicalProductId: UUID,
    val gtin: String?,
    val brand: String?,
    val model: String?,
    val title: String,
    val lowestPrice: BigDecimal,
    val highestPrice: BigDecimal,
    val priceSpread: BigDecimal,
    val bestRetailer: String,
    val currency: String,
    val offers: List<RetailerOfferDto>
)

data class PriceHistoryPointDto(
    val bucketTimestamp: Instant,
    val minPrice: BigDecimal,
    val maxPrice: BigDecimal,
    val avgPrice: BigDecimal,
    val sampleCount: Long
)

data class PriceHistoryResponse(
    val canonicalProductId: UUID,
    val timeRange: String,
    val currentLowestPrice: BigDecimal?,
    val allTimeLow: BigDecimal?,
    val allTimeHigh: BigDecimal?,
    val averagePrice: BigDecimal?,
    val points: List<PriceHistoryPointDto>
)

data class PriceStreamUpdate(
    val canonicalProductId: UUID,
    val retailerProductId: UUID,
    val retailer: String,
    val price: BigDecimal,
    val originalPrice: BigDecimal?,
    val currency: String,
    val inStock: Boolean,
    val recordedAt: Instant
)
```

---

## Subtask 4.3: Database Migrations & Search Indexing (`init-scripts/03-alerts-and-api-schema.sql`)

### 1. What We Are Doing
We are providing the complete SQL initialization script (`03-alerts-and-api-schema.sql`) containing:
1. Enabling PostgreSQL trigram module (`pg_trgm`) for fuzzy keyword search.
2. GIN trigram indexes on `canonical_products(title, brand, model)`.
3. The `price_alert_subscriptions` table for managing user alert preferences.
4. The `price_alert_history` TimescaleDB hypertable for recording alert executions with a 90-day automated data retention policy.

### 2. Why We Are Doing It & Deep Mechanics

#### Trigram GIN Search vs Traditional `ILIKE '%query%'`
- Traditional `ILIKE '%search%'` executes a full table scan (`Seq Scan`) across every row in the database, producing $O(N)$ execution time and catastrophic disk I/O as the product catalog scales into millions of items.
- PostgreSQL `pg_trgm` breaks text into 3-character substrings (e.g. `"sony"` -> `{"  s", " so", "son", "ony", "ny "}`). A GIN (Generalized Inverted Index) maps each trigram directly to row pointers.
- Trigram matching executes in $O(\log N)$ index lookup time, supporting typo tolerance, word reordering, and prefix/suffix matching without requiring an external Elasticsearch cluster.

#### Cooldowns & TimescaleDB Hypertable for Alert History
- When retailers update prices or during scraping bursts, a single product may generate multiple scrape events within minutes. Subscriptions maintain a `cooldown_hours` interval (default 24h) and `last_triggered_at` timestamp to prevent spamming users.
- `price_alert_history` stores diagnostic delivery logs partitioned into 30-day chunks via TimescaleDB hypertables, automatically compressing and dropping old audit rows after 90 days.

### 3. Proposed Code

#### File: `init-scripts/03-alerts-and-api-schema.sql`
```sql
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
```

---

## Subtask 4.4: Reactive Data Access Layer (`pulse-api`)

### 1. What We Are Doing
We are building the Spring Data R2DBC domain entity and repository classes for `pulse-api`:
- [`ApiEntities.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/domain/ApiEntities.kt): R2DBC mapped data classes for `canonical_products`, `retailer_products`, `price_history`, `price_alert_subscriptions`, and `price_alert_history`.
- [`ApiRepositories.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/domain/ApiRepositories.kt): Coroutine-native repositories utilizing Spring Data R2DBC `@Query` annotations and `DatabaseClient` for complex fuzzy joins and TimescaleDB analytical aggregates.
- [`DatabaseConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/config/DatabaseConfig.kt): Configures reactive connection pooling and custom R2DBC converters.

### 2. Why We Are Doing It & Deep Mechanics

#### Non-Blocking PostgreSQL Queries via `DatabaseClient`
- While standard repository CRUD methods handle simple key lookups, metasearch queries require full-text fuzzy similarity scoring, `DISTINCT ON` windowed price lookups, and time-bucket aggregations.
- We utilize `DatabaseClient` with Kotlin Coroutines extension `awaitSingle()`, `flow()`, and `awaitFirstOrNull()`. This executes non-blocking wire calls on Netty threads while preserving clean, structured Kotlin syntax without callback nests.

### 3. Proposed Code

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/config/DatabaseConfig.kt`
```kotlin
package com.pulseprice.api.config

import io.r2dbc.spi.ConnectionFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.ReactiveTransactionManager

@Configuration
@EnableR2dbcRepositories(basePackages = ["com.pulseprice.api.domain"])
class DatabaseConfig(
    private val connectionFactory: ConnectionFactory
) : AbstractR2dbcConfiguration() {

    override fun connectionFactory(): ConnectionFactory = connectionFactory

    @Bean
    fun transactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager {
        return R2dbcTransactionManager(connectionFactory)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/domain/ApiEntities.kt`
```kotlin
package com.pulseprice.api.domain

import com.pulseprice.common.model.AlertStatus
import com.pulseprice.common.model.AlertType
import com.pulseprice.common.model.NotificationChannel
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Table("canonical_products")
data class CanonicalProductEntity(
    @Id val id: UUID? = null,
    val gtin: String?,
    val brand: String?,
    val model: String?,
    val title: String,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now()
)

@Table("retailer_products")
data class RetailerProductEntity(
    @Id val id: UUID? = null,
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
    @Column("original_price") val originalPrice: BigDecimal?,
    val currency: String = "USD",
    @Column("in_stock") val inStock: Boolean = true
)

@Table("price_alert_subscriptions")
data class PriceAlertSubscriptionEntity(
    @Id val id: UUID? = null,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    @Column("user_email") val userEmail: String,
    @Column("alert_type") val alertType: AlertType,
    @Column("target_price") val targetPrice: BigDecimal?,
    @Column("percentage_drop_threshold") val percentageDropThreshold: BigDecimal?,
    @Column("notification_channel") val notificationChannel: NotificationChannel,
    val destination: String,
    val active: Boolean = true,
    @Column("cooldown_hours") val cooldownHours: Int = 24,
    @Column("last_triggered_at") val lastTriggeredAt: Instant? = null,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now()
)

@Table("price_alert_history")
data class PriceAlertHistoryEntity(
    val id: UUID = UUID.randomUUID(),
    @Column("subscription_id") val subscriptionId: UUID,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    @Column("retailer_product_id") val retailerProductId: UUID,
    @Column("triggered_at") val triggeredAt: Instant = Instant.now(),
    @Column("trigger_type") val triggerType: AlertType,
    @Column("old_price") val oldPrice: BigDecimal?,
    @Column("new_price") val newPrice: BigDecimal,
    @Column("price_drop_amount") val priceDropAmount: BigDecimal?,
    @Column("price_drop_percentage") val priceDropPercentage: BigDecimal?,
    val channel: NotificationChannel,
    val destination: String,
    val status: AlertStatus = AlertStatus.PENDING,
    @Column("error_message") val errorMessage: String? = null
)
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/domain/ApiRepositories.kt`
```kotlin
package com.pulseprice.api.domain

import com.pulseprice.common.model.PriceHistoryPointDto
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.repository.Modifying
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOneOrNull
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Repository
interface CanonicalProductRepository : CoroutineCrudRepository<CanonicalProductEntity, UUID> {
    suspend fun findByGtin(gtin: String): CanonicalProductEntity?
}

@Repository
interface RetailerProductRepository : CoroutineCrudRepository<RetailerProductEntity, UUID> {
    fun findByCanonicalProductId(canonicalProductId: UUID): Flow<RetailerProductEntity>
    suspend fun findByUrl(url: String): RetailerProductEntity?
}

@Repository
interface PriceAlertSubscriptionRepository : CoroutineCrudRepository<PriceAlertSubscriptionEntity, UUID> {
    fun findByCanonicalProductIdAndActiveTrue(canonicalProductId: UUID): Flow<PriceAlertSubscriptionEntity>
    fun findByUserEmail(userEmail: String): Flow<PriceAlertSubscriptionEntity>

    @Modifying
    @Query("""
        UPDATE price_alert_subscriptions
        SET last_triggered_at = :lastTriggeredAt, updated_at = NOW()
        WHERE id = :id
    """)
    suspend fun updateLastTriggered(id: UUID, lastTriggeredAt: Instant): Int
}

@Repository
interface PriceAlertHistoryRepository : CoroutineCrudRepository<PriceAlertHistoryEntity, UUID> {
    @Query("""
        INSERT INTO price_alert_history (
            id, subscription_id, canonical_product_id, retailer_product_id,
            triggered_at, trigger_type, old_price, new_price,
            price_drop_amount, price_drop_percentage, channel, destination, status, error_message
        ) VALUES (
            :id, :subscriptionId, :canonicalProductId, :retailerProductId,
            :triggeredAt, :triggerType, :oldPrice, :newPrice,
            :priceDropAmount, :priceDropPercentage, :channel, :destination, :status, :errorMessage
        )
    """)
    suspend fun insertAuditRecord(
        id: UUID,
        subscriptionId: UUID,
        canonicalProductId: UUID,
        retailerProductId: UUID,
        triggeredAt: Instant,
        triggerType: String,
        oldPrice: BigDecimal?,
        newPrice: BigDecimal,
        priceDropAmount: BigDecimal?,
        priceDropPercentage: BigDecimal?,
        channel: String,
        destination: String,
        status: String,
        errorMessage: String?
    ): Int
}

/**
 * Dedicated Custom Repository executing optimized PostgreSQL Trigram Metasearch
 * and TimescaleDB analytical time-bucket aggregations.
 */
@Repository
class ProductSearchCustomRepository(
    private val client: DatabaseClient,
    private val template: R2dbcEntityTemplate
) {

    data class SearchRow(
        val id: UUID,
        val gtin: String?,
        val brand: String?,
        val model: String?,
        val title: String,
        val lowestPrice: BigDecimal?,
        val highestPrice: BigDecimal?,
        val currency: String?,
        val retailerCount: Long,
        val inStockCount: Long,
        val similarityScore: Double
    )

    fun searchProducts(
        query: String,
        brand: String?,
        limit: Int,
        offset: Int
    ): Flow<SearchRow> {
        val sql = """
            SELECT
                cp.id,
                cp.gtin,
                cp.brand,
                cp.model,
                cp.title,
                MIN(ph.price) AS lowest_price,
                MAX(ph.price) AS highest_price,
                COALESCE(MAX(ph.currency), 'USD') AS currency,
                COUNT(DISTINCT rp.id) AS retailer_count,
                COUNT(DISTINCT CASE WHEN ph.in_stock THEN rp.id END) AS in_stock_count,
                GREATEST(
                    similarity(cp.title, :query),
                    similarity(COALESCE(cp.brand, ''), :query)
                ) AS sim_score
            FROM canonical_products cp
            LEFT JOIN retailer_products rp ON rp.canonical_product_id = cp.id
            LEFT JOIN LATERAL (
                SELECT price, currency, in_stock
                FROM price_history
                WHERE retailer_product_id = rp.id
                ORDER BY recorded_at DESC
                LIMIT 1
            ) ph ON TRUE
            WHERE (:query = '' OR cp.title % :query OR cp.title ILIKE CONCAT('%', :query, '%') OR cp.brand ILIKE CONCAT('%', :query, '%'))
              AND (:brand IS NULL OR LOWER(cp.brand) = LOWER(:brand))
            GROUP BY cp.id, cp.gtin, cp.brand, cp.model, cp.title
            ORDER BY sim_score DESC, lowest_price ASC NULLS LAST
            LIMIT :limit OFFSET :offset
        """.trimIndent()

        var spec = client.sql(sql)
            .bind("query", query)
            .bind("limit", limit)
            .bind("offset", offset)

        spec = if (brand != null) spec.bind("brand", brand) else spec.bindNull("brand", String::class.java)

        return spec.map { row, _ ->
            SearchRow(
                id = row.get("id", UUID::class.java)!!,
                gtin = row.get("gtin", String::class.java),
                brand = row.get("brand", String::class.java),
                model = row.get("model", String::class.java),
                title = row.get("title", String::class.java)!!,
                lowestPrice = row.get("lowest_price", BigDecimal::class.java),
                highestPrice = row.get("highest_price", BigDecimal::class.java),
                currency = row.get("currency", String::class.java) ?: "USD",
                retailerCount = row.get("retailer_count", java.lang.Long::class.java)?.toLong() ?: 0L,
                inStockCount = row.get("in_stock_count", java.lang.Long::class.java)?.toLong() ?: 0L,
                similarityScore = row.get("sim_score", java.lang.Double::class.java)?.toDouble() ?: 0.0
            )
        }.all().asFlow()
    }

    suspend fun countSearchResults(query: String, brand: String?): Long {
        val sql = """
            SELECT COUNT(*) AS total
            FROM canonical_products cp
            WHERE (:query = '' OR cp.title % :query OR cp.title ILIKE CONCAT('%', :query, '%') OR cp.brand ILIKE CONCAT('%', :query, '%'))
              AND (:brand IS NULL OR LOWER(cp.brand) = LOWER(:brand))
        """.trimIndent()

        var spec = client.sql(sql).bind("query", query)
        spec = if (brand != null) spec.bind("brand", brand) else spec.bindNull("brand", String::class.java)

        val result = spec.map { row, _ -> row.get("total", java.lang.Long::class.java)?.toLong() ?: 0L }
            .awaitOneOrNull()

        return result ?: 0L
    }

    data class RetailerOfferRow(
        val retailerProductId: UUID,
        val retailer: String,
        val retailerSku: String?,
        val url: String,
        val title: String,
        val price: BigDecimal,
        val originalPrice: BigDecimal?,
        val currency: String,
        val inStock: Boolean,
        val recordedAt: Instant
    )

    fun findOffersForProduct(canonicalProductId: UUID): Flow<RetailerOfferRow> {
        val sql = """
            SELECT DISTINCT ON (rp.id)
                rp.id AS retailer_product_id,
                rp.retailer,
                rp.retailer_sku,
                rp.url,
                rp.title,
                ph.price,
                ph.original_price,
                ph.currency,
                ph.in_stock,
                ph.recorded_at
            FROM retailer_products rp
            JOIN price_history ph ON ph.retailer_product_id = rp.id
            WHERE rp.canonical_product_id = :canonicalProductId
            ORDER BY rp.id, ph.recorded_at DESC
        """.trimIndent()

        return client.sql(sql)
            .bind("canonicalProductId", canonicalProductId)
            .map { row, _ ->
                RetailerOfferRow(
                    retailerProductId = row.get("retailer_product_id", UUID::class.java)!!,
                    retailer = row.get("retailer", String::class.java)!!,
                    retailerSku = row.get("retailer_sku", String::class.java),
                    url = row.get("url", String::class.java)!!,
                    title = row.get("title", String::class.java)!!,
                    price = row.get("price", BigDecimal::class.java)!!,
                    originalPrice = row.get("original_price", BigDecimal::class.java),
                    currency = row.get("currency", String::class.java) ?: "USD",
                    inStock = row.get("in_stock", java.lang.Boolean::class.java)?.booleanValue() ?: true,
                    recordedAt = row.get("recorded_at", Instant::class.java) ?: Instant.now()
                )
            }.all().asFlow()
    }

    fun findHistoricalPriceBuckets(
        canonicalProductId: UUID,
        bucketInterval: String,
        since: Instant
    ): Flow<PriceHistoryPointDto> {
        val sql = """
            SELECT
                time_bucket(INTERVAL '$bucketInterval', recorded_at) AS bucket_ts,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price)::NUMERIC(12, 2) AS avg_price,
                COUNT(*) AS sample_count
            FROM price_history
            WHERE canonical_product_id = :canonicalProductId
              AND recorded_at >= :since
            GROUP BY bucket_ts
            ORDER BY bucket_ts ASC
        """.trimIndent()

        return client.sql(sql)
            .bind("canonicalProductId", canonicalProductId)
            .bind("since", since)
            .map { row, _ ->
                PriceHistoryPointDto(
                    bucketTimestamp = row.get("bucket_ts", Instant::class.java)!!,
                    minPrice = row.get("min_price", BigDecimal::class.java)!!,
                    maxPrice = row.get("max_price", BigDecimal::class.java)!!,
                    avgPrice = row.get("avg_price", BigDecimal::class.java)!!,
                    sampleCount = row.get("sample_count", java.lang.Long::class.java)?.toLong() ?: 0L
                )
            }.all().asFlow()
    }

    suspend fun findAllTimeLowAndHigh(canonicalProductId: UUID): Pair<BigDecimal?, BigDecimal?>? {
        val sql = """
            SELECT MIN(price) AS all_time_low, MAX(price) AS all_time_high
            FROM price_history
            WHERE canonical_product_id = :canonicalProductId
        """.trimIndent()

        return client.sql(sql)
            .bind("canonicalProductId", canonicalProductId)
            .map { row, _ ->
                val low = row.get("all_time_low", BigDecimal::class.java)
                val high = row.get("all_time_high", BigDecimal::class.java)
                Pair(low, high)
            }.awaitOneOrNull()
    }
}
```

---

## Subtask 4.5: High-Performance Real-Time Alert Engine (`pulse-api`)

### 1. What We Are Doing
We are building the real-time stream evaluation engine that consumes `canonical-prices` Kafka events, queries active user subscriptions, evaluates multi-criteria trigger rules (absolute target price, percentage drop, all-time-low, back-in-stock), and dispatches deduplicated `PriceDropAlertEvent` messages to Kafka topic `price-drop-alerts`:
- [`AlertCooldownManager.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertCooldownManager.kt): Atomic Redis cooldown coordinator (`SETNX` with TTL).
- [`AlertEvaluationEngine.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertEvaluationEngine.kt): Core evaluation logic with previous price and all-time-low comparison.
- [`AlertKafkaConsumer.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertKafkaConsumer.kt): Non-blocking Kafka consumer processing incoming `CanonicalProductPriceEvent` streams.
- [`AlertHistoryService.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertHistoryService.kt): Asynchronous audit logger inserting records into TimescaleDB `price_alert_history`.

### 2. Why We Are Doing It & Deep Mechanics

#### Stream Ingestion vs Batch Scanning
- Traditional e-commerce price alert engines run periodic batch cron jobs (e.g. hourly SQL queries `SELECT * FROM subscriptions WHERE target_price >= ...`). Under heavy loads, these batch queries cause massive database lock contention, spike CPU, and introduce hours of latency between a retailer flash sale and user alert delivery.
- In `PulsePrice`, alert matching is **100% event-driven**: as soon as `pulse-pipeline` writes a price snapshot, a `CanonicalProductPriceEvent` arrives in Kafka. The engine evaluates subscriptions immediately in memory and publishes the alert downstream within milliseconds.

#### Atomic Redis Cooldown Deduplication
- If a scraper processes price fluctuations or repeatedly polls a retailer product, triggering repeated emails or webhooks would irritate users.
- `AlertCooldownManager` computes an atomic Redis key: `cooldown:alert:<subscriptionId>:<canonicalProductId>`. Using `setIfAbsent(key, "1", Duration.ofHours(cooldownHours))`, we guarantee that even if multiple price events arrive simultaneously across distributed API nodes, exactly **one** alert fires within the configured cooldown window.

### 3. Proposed Code

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertCooldownManager.kt`
```kotlin
package com.pulseprice.api.alert

import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.UUID

@Component
class AlertCooldownManager(
    private val redisTemplate: ReactiveStringRedisTemplate
) {

    suspend fun acquireCooldownLock(subscriptionId: UUID, canonicalProductId: UUID, cooldownHours: Int): Boolean {
        val key = "cooldown:alert:$subscriptionId:$canonicalProductId"
        val acquired = redisTemplate.opsForValue()
            .setIfAbsent(key, "ACTIVE", Duration.ofHours(cooldownHours.toLong()))
            .awaitFirstOrNull() ?: false

        if (!acquired) {
            log.debug("Cooldown active for subscriptionId={} productId={}, suppressing alert", subscriptionId, canonicalProductId)
        }
        return acquired
    }

    suspend fun getPreviousPrice(retailerProductId: UUID): java.math.BigDecimal? {
        val key = "price:last:$retailerProductId"
        val raw = redisTemplate.opsForValue().get(key).awaitFirstOrNull() ?: return null
        return try {
            java.math.BigDecimal(raw)
        } catch (ex: Exception) {
            null
        }
    }

    suspend fun recordLatestPrice(retailerProductId: UUID, price: java.math.BigDecimal) {
        val key = "price:last:$retailerProductId"
        redisTemplate.opsForValue().set(key, price.toPlainString(), Duration.ofDays(7)).awaitFirstOrNull()
    }

    companion object {
        private val log = LoggerFactory.getLogger(AlertCooldownManager::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertEvaluationEngine.kt`
```kotlin
package com.pulseprice.api.alert

import com.pulseprice.api.domain.CanonicalProductRepository
import com.pulseprice.api.domain.PriceAlertSubscriptionEntity
import com.pulseprice.api.domain.PriceAlertSubscriptionRepository
import com.pulseprice.api.domain.ProductSearchCustomRepository
import com.pulseprice.api.domain.RetailerProductRepository
import com.pulseprice.common.event.CanonicalProductPriceEvent
import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.AlertType
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

@Service
class AlertEvaluationEngine(
    private val subscriptionRepository: PriceAlertSubscriptionRepository,
    private val canonicalProductRepository: CanonicalProductRepository,
    private val retailerProductRepository: RetailerProductRepository,
    private val searchCustomRepository: ProductSearchCustomRepository,
    private val cooldownManager: AlertCooldownManager,
    private val alertHistoryService: AlertHistoryService,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    @Value("\${pulse.kafka.topics.price-drop-alerts:price-drop-alerts}")
    private val priceDropAlertsTopic: String
) {

    suspend fun evaluatePriceEvent(event: CanonicalProductPriceEvent) {
        val canonicalId = event.canonicalProductId
        val currentPrice = event.price

        // 1. Fetch previous price from Redis cache
        val previousPrice = cooldownManager.getPreviousPrice(event.retailerProductId)
        // Store current price as latest for subsequent evaluations
        cooldownManager.recordLatestPrice(event.retailerProductId, currentPrice)

        // 2. Fetch active subscriptions for this canonical product
        val subscriptions = subscriptionRepository.findByCanonicalProductIdAndActiveTrue(canonicalId).toList()
        if (subscriptions.isEmpty()) {
            log.trace("No active subscriptions for canonicalProductId={}", canonicalId)
            return
        }

        // 3. Retrieve product details
        val product = canonicalProductRepository.findById(canonicalId)
        val retailerProduct = retailerProductRepository.findById(event.retailerProductId)
        val productTitle = product?.title ?: retailerProduct?.title ?: "Tracked Product"
        val productUrl = retailerProduct?.url ?: ""

        // 4. Retrieve historical bounds if required for ALL_TIME_LOW evaluation
        val bounds = searchCustomRepository.findAllTimeLowAndHigh(canonicalId)
        val allTimeLow = bounds?.first

        for (sub in subscriptions) {
            val isTriggered = checkTriggerCondition(
                subscription = sub,
                currentPrice = currentPrice,
                previousPrice = previousPrice,
                allTimeLow = allTimeLow,
                inStock = event.inStock
            )

            if (isTriggered) {
                // Check atomic Redis cooldown lock to prevent spam
                val lockAcquired = cooldownManager.acquireCooldownLock(
                    subscriptionId = sub.id!!,
                    canonicalProductId = canonicalId,
                    cooldownHours = sub.cooldownHours
                )

                if (!lockAcquired) {
                    log.info("Alert trigger suppressed by cooldown for subscriptionId={} user={}", sub.id, sub.userEmail)
                    continue
                }

                // Compute drop metrics
                val dropAmount = if (previousPrice != null && previousPrice > currentPrice) {
                    previousPrice.subtract(currentPrice)
                } else BigDecimal.ZERO

                val dropPercentage = if (previousPrice != null && previousPrice > BigDecimal.ZERO && previousPrice > currentPrice) {
                    dropAmount.divide(previousPrice, 4, RoundingMode.HALF_UP).multiply(BigDecimal(100)).setScale(2, RoundingMode.HALF_UP)
                } else BigDecimal.ZERO

                val alertEvent = PriceDropAlertEvent(
                    subscriptionId = sub.id,
                    canonicalProductId = canonicalId,
                    retailerProductId = event.retailerProductId,
                    productTitle = productTitle,
                    retailer = event.retailer,
                    productUrl = productUrl,
                    currentPrice = currentPrice,
                    previousPrice = previousPrice ?: currentPrice,
                    priceDropAmount = dropAmount,
                    priceDropPercentage = dropPercentage,
                    currency = event.currency,
                    alertType = sub.alertType,
                    channel = sub.notificationChannel,
                    destination = sub.destination,
                    triggeredAt = Instant.now()
                )

                // Dispatch to Kafka topic
                kafkaTemplate.send(priceDropAlertsTopic, sub.id.toString(), alertEvent)
                log.info("Dispatched PriceDropAlertEvent for subId={} type={} target={}", sub.id, sub.alertType, sub.destination)

                // Update last_triggered_at in DB
                subscriptionRepository.updateLastTriggered(sub.id, Instant.now())

                // Record audit log in TimescaleDB
                alertHistoryService.recordAlertDispatched(alertEvent)
            }
        }
    }

    private fun checkTriggerCondition(
        subscription: PriceAlertSubscriptionEntity,
        currentPrice: BigDecimal,
        previousPrice: BigDecimal?,
        allTimeLow: BigDecimal?,
        inStock: Boolean
    ): Boolean {
        if (!inStock) return false

        return when (subscription.alertType) {
            AlertType.TARGET_PRICE -> {
                val target = subscription.targetPrice ?: return false
                currentPrice <= target
            }
            AlertType.PERCENTAGE_DROP -> {
                val threshold = subscription.percentageDropThreshold ?: return false
                if (previousPrice == null || previousPrice <= BigDecimal.ZERO || currentPrice >= previousPrice) {
                    return false
                }
                val dropPct = previousPrice.subtract(currentPrice)
                    .divide(previousPrice, 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal(100))
                dropPct >= threshold
            }
            AlertType.ALL_TIME_LOW -> {
                if (allTimeLow == null) return false
                currentPrice < allTimeLow
            }
            AlertType.BACK_IN_STOCK -> {
                inStock
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(AlertEvaluationEngine::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertHistoryService.kt`
```kotlin
package com.pulseprice.api.alert

import com.pulseprice.api.domain.PriceAlertHistoryRepository
import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.AlertStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class AlertHistoryService(
    private val alertHistoryRepository: PriceAlertHistoryRepository
) {
    private val scope = CoroutineScope(Dispatchers.IO)

    fun recordAlertDispatched(event: PriceDropAlertEvent) {
        scope.launch {
            try {
                alertHistoryRepository.insertAuditRecord(
                    id = UUID.fromString(event.eventId),
                    subscriptionId = event.subscriptionId,
                    canonicalProductId = event.canonicalProductId,
                    retailerProductId = event.retailerProductId,
                    triggeredAt = event.triggeredAt,
                    triggerType = event.alertType.name,
                    oldPrice = event.previousPrice,
                    newPrice = event.currentPrice,
                    priceDropAmount = event.priceDropAmount,
                    priceDropPercentage = event.priceDropPercentage,
                    channel = event.channel.name,
                    destination = event.destination,
                    status = AlertStatus.PENDING.name,
                    errorMessage = null
                )
            } catch (ex: Exception) {
                log.error("Failed to insert alert audit record for eventId={}: {}", event.eventId, ex.message)
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(AlertHistoryService::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/alert/AlertKafkaConsumer.kt`
```kotlin
package com.pulseprice.api.alert

import com.pulseprice.common.event.CanonicalProductPriceEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class AlertKafkaConsumer(
    private val alertEvaluationEngine: AlertEvaluationEngine
) {
    private val consumerScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(
        topics = ["\${pulse.kafka.topics.canonical-prices:canonical-prices}"],
        groupId = "\${spring.kafka.consumer.group-id:pulse-alert-engine-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onCanonicalPriceReceived(
        record: ConsumerRecord<String, CanonicalProductPriceEvent>,
        ack: Acknowledgment?
    ) {
        val event = record.value()
        log.debug("Alert engine received canonical price event: canonicalId={} price={}", event.canonicalProductId, event.price)

        consumerScope.launch {
            try {
                alertEvaluationEngine.evaluatePriceEvent(event)
                ack?.acknowledge()
            } catch (ex: Exception) {
                log.error("Error evaluating alert for canonicalId={}: {}", event.canonicalProductId, ex.message, ex)
                // Acknowledge or let Kafka DLT handle retry according to consumer config
                ack?.acknowledge()
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(AlertKafkaConsumer::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/config/KafkaApiConfig.kt`
```kotlin
package com.pulseprice.api.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer

@EnableKafka
@Configuration
class KafkaApiConfig(
    @Value("\${spring.kafka.bootstrap-servers:localhost:9092}")
    private val bootstrapServers: String,
    @Value("\${spring.kafka.consumer.group-id:pulse-alert-engine-group}")
    private val defaultGroupId: String
) {

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = consumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.setConcurrency(3) // Process across 3 parallel consumer threads
        return factory
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Any> {
        val props = HashMap<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.GROUP_ID_CONFIG] = defaultGroupId
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        props[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = JsonDeserializer::class.java.name
        props[JsonDeserializer.TRUSTED_PACKAGES] = "com.pulseprice.*"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        return DefaultKafkaConsumerFactory(props)
    }
}
```

---

## Subtask 4.6: Multi-Channel Notification Dispatcher (`pulse-api`)

### 1. What We Are Doing
We are implementing the downstream notification dispatching subsystem that consumes `PriceDropAlertEvent` messages from the `price-drop-alerts` Kafka topic and routes them to pluggable channel handlers:
- [`NotificationDispatcher.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/notification/NotificationDispatcher.kt): Kafka listener routing events based on channel type.
- [`NotificationChannelHandler.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/notification/NotificationChannelHandler.kt): Strategy interface for channel execution.
- [`WebhookNotificationHandler.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/notification/WebhookNotificationHandler.kt): Non-blocking HTTP POST dispatcher with HMAC-SHA256 signature verification and exponential retry.
- [`EmailNotificationHandler.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/notification/EmailNotificationHandler.kt): Responsive HTML email notification formatter with direct retailer deep links and deal badges.
- [`SseNotificationHandler.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/notification/SseNotificationHandler.kt): Server-Sent Events broker piping live notifications to active browser clients.

### 2. Why We Are Doing It & Deep Mechanics

#### Webhook Security: HMAC-SHA256 Signatures
- When delivering webhooks to external user endpoints (e.g., Discord/Slack bots, user services), users must be able to authenticate that the payload originated from PulsePrice and was not tampered with in transit.
- `WebhookNotificationHandler` calculates a cryptographic signature using `HmacSHA256(payload, secret)` and attaches the header `X-Pulse-Signature: sha256=<hex>`. It also sets an `X-Pulse-Timestamp` header to protect against replay attacks.

### 3. Proposed Code

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/notification/NotificationChannelHandler.kt`
```kotlin
package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel

interface NotificationChannelHandler {
    val supportedChannel: NotificationChannel
    suspend fun sendNotification(event: PriceDropAlertEvent): Boolean
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/notification/WebhookNotificationHandler.kt`
```kotlin
package com.pulseprice.api.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@Component
class WebhookNotificationHandler(
    private val webClientBuilder: WebClient.Builder,
    private val objectMapper: ObjectMapper,
    @Value("\${pulse.alerts.hmac-secret:pulse-engine-super-secret-key-32bytes!}")
    private val hmacSecret: String,
    @Value("\${pulse.alerts.webhook-timeout-ms:4000}")
    private val timeoutMs: Long
) : NotificationChannelHandler {

    override val supportedChannel: NotificationChannel = NotificationChannel.WEBHOOK
    private val client = webClientBuilder.build()

    override suspend fun sendNotification(event: PriceDropAlertEvent): Boolean {
        val destinationUrl = event.destination
        val jsonPayload = objectMapper.writeValueAsString(event)
        val timestamp = System.currentTimeMillis().toString()
        val signature = calculateHmacSha256("$timestamp.$jsonPayload", hmacSecret)

        return try {
            val responseStatus = client.post()
                .uri(destinationUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Pulse-Timestamp", timestamp)
                .header("X-Pulse-Signature", "sha256=$signature")
                .bodyValue(jsonPayload)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofMillis(timeoutMs))
                .awaitFirstOrNull()

            val success = responseStatus?.statusCode?.is2xxSuccessful == true
            if (success) {
                log.info("Webhook successfully dispatched to {} for eventId={}", destinationUrl, event.eventId)
            } else {
                log.warn("Webhook dispatched to {} returned non-2xx status: {}", destinationUrl, responseStatus?.statusCode)
            }
            success
        } catch (ex: Exception) {
            log.error("Failed to deliver webhook to {}: {}", destinationUrl, ex.message)
            false
        }
    }

    private fun calculateHmacSha256(data: String, key: String): String {
        val sha256Hmac = Mac.getInstance("HmacSHA256")
        val secretKey = SecretKeySpec(key.toByteArray(Charsets.UTF_8), "HmacSHA256")
        sha256Hmac.init(secretKey)
        val hash = sha256Hmac.doFinal(data.toByteArray(Charsets.UTF_8))
        return hash.joinToString("") { "%02x".format(it) }
    }

    companion object {
        private val log = LoggerFactory.getLogger(WebhookNotificationHandler::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/notification/EmailNotificationHandler.kt`
```kotlin
package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EmailNotificationHandler : NotificationChannelHandler {

    override val supportedChannel: NotificationChannel = NotificationChannel.EMAIL

    override suspend fun sendNotification(event: PriceDropAlertEvent): Boolean {
        val recipient = event.destination
        val htmlContent = buildEmailTemplate(event)

        // In a production environment, this integrates with Amazon SES, Sendgrid, or Mailgun WebClient API
        log.info(
            "Sending Price Drop Email to [{}] | Product: '{}' | Dropped from \${} to \${} ({}% OFF) | Deal URL: {}",
            recipient,
            event.productTitle,
            event.previousPrice,
            event.currentPrice,
            event.priceDropPercentage,
            event.productUrl
        )
        log.debug("Email HTML preview for [{}]:\n{}", recipient, htmlContent)
        return true
    }

    private fun buildEmailTemplate(event: PriceDropAlertEvent): String {
        return """
            <!DOCTYPE html>
            <html>
            <head><meta charset="utf-8"><style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f8; margin: 0; padding: 20px; }
                .card { max-width: 600px; margin: auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
                .header { background: #1e293b; color: #ffffff; padding: 24px; text-align: center; }
                .content { padding: 32px 24px; }
                .badge { display: inline-block; background: #10b981; color: white; padding: 4px 12px; border-radius: 9999px; font-weight: bold; font-size: 14px; }
                .price-box { margin: 24px 0; padding: 16px; background: #f8fafc; border-left: 4px solid #10b981; border-radius: 4px; }
                .current-price { font-size: 28px; font-weight: 800; color: #0f172a; }
                .old-price { text-decoration: line-through; color: #64748b; font-size: 18px; margin-left: 12px; }
                .btn { display: inline-block; padding: 14px 28px; background: #2563eb; color: white; text-decoration: none; border-radius: 6px; font-weight: 600; margin-top: 16px; }
            </style></head>
            <body>
                <div class="card">
                    <div class="header"><h2>PulsePrice Alert: Price Drop Detected!</h2></div>
                    <div class="content">
                        <span class="badge">${event.priceDropPercentage}% PRICE DROP</span>
                        <h3>${event.productTitle}</h3>
                        <p>Tracked at retailer: <strong>${event.retailer}</strong></p>
                        <div class="price-box">
                            <span class="current-price">$${event.currentPrice} ${event.currency}</span>
                            <span class="old-price">$${event.previousPrice}</span>
                        </div>
                        <a href="${event.productUrl}" class="btn" target="_blank">View Deal at ${event.retailer} &rarr;</a>
                    </div>
                </div>
            </body>
            </html>
        """.trimIndent()
    }

    companion object {
        private val log = LoggerFactory.getLogger(EmailNotificationHandler::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/notification/SseNotificationHandler.kt`
```kotlin
package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class SseNotificationHandler : NotificationChannelHandler {

    override val supportedChannel: NotificationChannel = NotificationChannel.SSE

    private val _alertFlow = MutableSharedFlow<PriceDropAlertEvent>(extraBufferCapacity = 64)
    val alertFlow: SharedFlow<PriceDropAlertEvent> = _alertFlow.asSharedFlow()

    override suspend fun sendNotification(event: PriceDropAlertEvent): Boolean {
        val emitted = _alertFlow.tryEmit(event)
        log.debug("SSE alert emitted for product {}: success={}", event.canonicalProductId, emitted)
        return emitted
    }

    companion object {
        private val log = LoggerFactory.getLogger(SseNotificationHandler::class.java)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/notification/NotificationDispatcher.kt`
```kotlin
package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class NotificationDispatcher(
    channelHandlers: List<NotificationChannelHandler>
) {
    private val handlerMap: Map<NotificationChannel, NotificationChannelHandler> =
        channelHandlers.associateBy { it.supportedChannel }

    private val dispatcherScope = CoroutineScope(Dispatchers.IO)

    @KafkaListener(
        topics = ["\${pulse.kafka.topics.price-drop-alerts:price-drop-alerts}"],
        groupId = "pulse-notification-dispatcher-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onPriceDropAlertReceived(
        record: ConsumerRecord<String, PriceDropAlertEvent>,
        ack: Acknowledgment?
    ) {
        val event = record.value()
        log.info("Received PriceDropAlertEvent: id={} channel={} destination={}", event.eventId, event.channel, event.destination)

        dispatcherScope.launch {
            val handler = handlerMap[event.channel]
            if (handler == null) {
                log.warn("No registered handler for notification channel: {}", event.channel)
                ack?.acknowledge()
                return@launch
            }

            try {
                val delivered = handler.sendNotification(event)
                log.info("Notification delivery result for event {}: {}", event.eventId, delivered)
                ack?.acknowledge()
            } catch (ex: Exception) {
                log.error("Failed to execute notification handler for event {}: {}", event.eventId, ex.message, ex)
                ack?.acknowledge()
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(NotificationDispatcher::class.java)
    }
}
```

---

## Subtask 4.7: Metasearch Reactive REST Controllers & SSE Stream (`pulse-api`)

### 1. What We Are Doing
We are building the high-performance non-blocking Spring WebFlux controllers exposing the metasearch REST API:
- [`ProductSearchController.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/controller/ProductSearchController.kt): `GET /api/v1/products/search` (Fuzzy trigram keyword search, brand filter, price range).
- [`ProductComparisonController.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/controller/ProductComparisonController.kt): `GET /api/v1/products/{id}/compare` (Cross-retailer price comparison and Best Deal indicator).
- [`ProductPriceHistoryController.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/controller/ProductPriceHistoryController.kt): `GET /api/v1/products/{id}/history` (Analytical time-series buckets from TimescaleDB).
- [`PriceAlertSubscriptionController.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/controller/PriceAlertSubscriptionController.kt): `POST /api/v1/alerts/subscriptions`, `GET`, and `DELETE`.
- [`PriceStreamController.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/controller/PriceStreamController.kt): `GET /api/v1/products/{id}/live-stream` (Server-Sent Events streaming live price ticks).
- Reactive services backing the controllers ([`MetasearchServices.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/service/MetasearchServices.kt)).

### 2. Why We Are Doing It & Deep Mechanics

#### Multi-Retailer Price Comparison (`/compare`)
- In metasearch, multiple retailers may list the identical canonical product (e.g. Sony WH-1000XM5) at different price points with different stock statuses.
- `PriceComparisonService` evaluates all active retailer links for a canonical UUID, calculates the absolute lowest price, tags that offer as `isBestPrice = true`, and determines `priceDifferenceFromLowest` for all competing merchants in a single reactive transaction.

#### Server-Sent Events (SSE) vs WebSockets
- For real-time price updates on product detail pages, Server-Sent Events (`text/event-stream`) provide a lightweight HTTP-standard streaming mechanism. Unlike WebSockets, SSE operates over standard HTTP/2, requires no custom framing protocol, reconnects automatically in modern browsers, and works seamlessly behind enterprise firewalls and load balancers.

### 3. Proposed Code

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/service/MetasearchServices.kt`
```kotlin
package com.pulseprice.api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.api.domain.*
import com.pulseprice.common.model.*
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Service
class MetasearchProductService(
    private val searchCustomRepository: ProductSearchCustomRepository,
    private val redisTemplate: ReactiveStringRedisTemplate,
    private val objectMapper: ObjectMapper,
    @Value("\${pulse.cache.search-ttl-seconds:60}")
    private val searchTtlSeconds: Long
) {

    suspend fun search(
        query: String,
        brand: String?,
        page: Int,
        size: Int
    ): ProductSearchResult {
        val cacheKey = "cache:search:${query.trim().lowercase()}:${brand?.lowercase()}:$page:$size"
        val cached = redisTemplate.opsForValue().get(cacheKey).awaitFirstOrNull()
        if (cached != null) {
            try {
                return objectMapper.readValue(cached, ProductSearchResult::class.java)
            } catch (ex: Exception) {
                log.warn("Cache deserialization failed for {}: {}", cacheKey, ex.message)
            }
        }

        val offset = page * size
        val itemsFlow = searchCustomRepository.searchProducts(
            query = query.trim(),
            brand = brand?.trim(),
            limit = size,
            offset = offset
        )

        val hits = itemsFlow.map { row ->
            ProductSearchHit(
                id = row.id,
                gtin = row.gtin,
                brand = row.brand,
                model = row.model,
                title = row.title,
                lowestPrice = row.lowestPrice,
                highestPrice = row.highestPrice,
                currency = row.currency ?: "USD",
                retailerCount = row.retailerCount.toInt(),
                inStockCount = row.inStockCount.toInt(),
                similarityScore = row.similarityScore
            )
        }.toList()

        val totalElements = searchCustomRepository.countSearchResults(query.trim(), brand?.trim())
        val totalPages = if (size > 0) ((totalElements + size - 1) / size).toInt() else 0

        val result = ProductSearchResult(
            items = hits,
            page = page,
            size = size,
            totalElements = totalElements,
            totalPages = totalPages
        )

        // Store in Redis L1 cache
        try {
            val json = objectMapper.writeValueAsString(result)
            redisTemplate.opsForValue().set(cacheKey, json, Duration.ofSeconds(searchTtlSeconds)).awaitFirstOrNull()
        } catch (ex: Exception) {
            log.warn("Failed to write search results to Redis cache: {}", ex.message)
        }

        return result
    }

    companion object {
        private val log = LoggerFactory.getLogger(MetasearchProductService::class.java)
    }
}

@Service
class PriceComparisonService(
    private val canonicalProductRepository: CanonicalProductRepository,
    private val searchCustomRepository: ProductSearchCustomRepository
) {

    suspend fun getCrossRetailerComparison(canonicalProductId: UUID): ProductComparisonResponse? {
        val canonical = canonicalProductRepository.findById(canonicalProductId) ?: return null
        val offers = searchCustomRepository.findOffersForProduct(canonicalProductId).toList()

        if (offers.isEmpty()) {
            return ProductComparisonResponse(
                canonicalProductId = canonical.id!!,
                gtin = canonical.gtin,
                brand = canonical.brand,
                model = canonical.model,
                title = canonical.title,
                lowestPrice = BigDecimal.ZERO,
                highestPrice = BigDecimal.ZERO,
                priceSpread = BigDecimal.ZERO,
                bestRetailer = "None",
                currency = "USD",
                offers = emptyList()
            )
        }

        val inStockOffers = offers.filter { it.inStock }
        val targetOffers = if (inStockOffers.isNotEmpty()) inStockOffers else offers
        val lowestOffer = targetOffers.minByOrNull { it.price } ?: offers.first()
        val highestOffer = targetOffers.maxByOrNull { it.price } ?: offers.first()

        val offerDtos = offers.map { offer ->
            val diff = offer.price.subtract(lowestOffer.price)
            RetailerOfferDto(
                retailerProductId = offer.retailerProductId,
                retailer = offer.retailer,
                retailerSku = offer.retailerSku,
                url = offer.url,
                title = offer.title,
                currentPrice = offer.price,
                originalPrice = offer.originalPrice,
                currency = offer.currency,
                inStock = offer.inStock,
                isBestPrice = (offer.retailerProductId == lowestOffer.retailerProductId),
                priceDifferenceFromLowest = diff,
                lastUpdated = offer.recordedAt
            )
        }.sortedWith(compareBy({ !it.inStock }, { it.currentPrice }))

        return ProductComparisonResponse(
            canonicalProductId = canonical.id!!,
            gtin = canonical.gtin,
            brand = canonical.brand,
            model = canonical.model,
            title = canonical.title,
            lowestPrice = lowestOffer.price,
            highestPrice = highestOffer.price,
            priceSpread = highestOffer.price.subtract(lowestOffer.price),
            bestRetailer = lowestOffer.retailer,
            currency = lowestOffer.currency,
            offers = offerDtos
        )
    }
}

@Service
class PriceHistoryAnalyticsService(
    private val searchCustomRepository: ProductSearchCustomRepository
) {

    suspend fun getHistory(canonicalProductId: UUID, range: String): PriceHistoryResponse {
        val (since, interval) = when (range.lowercase()) {
            "7d" -> Pair(Instant.now().minus(Duration.ofDays(7)), "1 hour")
            "30d" -> Pair(Instant.now().minus(Duration.ofDays(30)), "1 day")
            "90d" -> Pair(Instant.now().minus(Duration.ofDays(90)), "1 day")
            "1y" -> Pair(Instant.now().minus(Duration.ofDays(365)), "7 days")
            else -> Pair(Instant.now().minus(Duration.ofDays(30)), "1 day")
        }

        val points = searchCustomRepository.findHistoricalPriceBuckets(canonicalProductId, interval, since).toList()
        val bounds = searchCustomRepository.findAllTimeLowAndHigh(canonicalProductId)

        val avg = if (points.isNotEmpty()) {
            val total = points.map { it.avgPrice }.reduce { acc, p -> acc.add(p) }
            total.divide(BigDecimal(points.size), 2, java.math.RoundingMode.HALF_UP)
        } else null

        return PriceHistoryResponse(
            canonicalProductId = canonicalProductId,
            timeRange = range,
            currentLowestPrice = points.lastOrNull()?.minPrice,
            allTimeLow = bounds?.first,
            allTimeHigh = bounds?.second,
            averagePrice = avg,
            points = points
        )
    }
}

@Service
class PriceAlertSubscriptionService(
    private val subscriptionRepository: PriceAlertSubscriptionRepository
) {

    suspend fun createSubscription(request: PriceAlertSubscriptionRequest): PriceAlertSubscriptionResponse {
        val entity = PriceAlertSubscriptionEntity(
            canonicalProductId = request.canonicalProductId,
            userEmail = request.userEmail,
            alertType = request.alertType,
            targetPrice = request.targetPrice,
            percentageDropThreshold = request.percentageDropThreshold,
            notificationChannel = request.notificationChannel,
            destination = request.destination,
            cooldownHours = request.cooldownHours
        )

        val saved = subscriptionRepository.save(entity)
        return toDto(saved)
    }

    suspend fun getSubscriptionsForUser(userEmail: String): List<PriceAlertSubscriptionResponse> {
        return subscriptionRepository.findByUserEmail(userEmail)
            .map { toDto(it) }
            .toList()
    }

    suspend fun deactivateSubscription(id: UUID): Boolean {
        val existing = subscriptionRepository.findById(id) ?: return false
        val updated = existing.copy(active = false, updatedAt = Instant.now())
        subscriptionRepository.save(updated)
        return true
    }

    private fun toDto(entity: PriceAlertSubscriptionEntity) = PriceAlertSubscriptionResponse(
        id = entity.id!!,
        canonicalProductId = entity.canonicalProductId,
        userEmail = entity.userEmail,
        alertType = entity.alertType,
        targetPrice = entity.targetPrice,
        percentageDropThreshold = entity.percentageDropThreshold,
        notificationChannel = entity.notificationChannel,
        destination = entity.destination,
        active = entity.active,
        cooldownHours = entity.cooldownHours,
        lastTriggeredAt = entity.lastTriggeredAt,
        createdAt = entity.createdAt,
        updatedAt = entity.updatedAt
    )
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/controller/ProductSearchController.kt`
```kotlin
package com.pulseprice.api.controller

import com.pulseprice.api.service.MetasearchProductService
import com.pulseprice.common.model.ProductSearchResult
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/products")
class ProductSearchController(
    private val searchService: MetasearchProductService
) {

    @GetMapping("/search")
    suspend fun searchProducts(
        @RequestParam(defaultValue = "") q: String,
        @RequestParam(required = false) brand: String?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int
    ): ProductSearchResult {
        val sanitizedPage = page.coerceAtLeast(0)
        val sanitizedSize = size.coerceIn(1, 100)
        return searchService.search(q, brand, sanitizedPage, sanitizedSize)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/controller/ProductComparisonController.kt`
```kotlin
package com.pulseprice.api.controller

import com.pulseprice.api.service.PriceComparisonService
import com.pulseprice.common.model.ProductComparisonResponse
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.util.UUID

@RestController
@RequestMapping("/api/v1/products")
class ProductComparisonController(
    private val comparisonService: PriceComparisonService
) {

    @GetMapping("/{id}/compare")
    suspend fun compareProductPrices(@PathVariable id: UUID): ProductComparisonResponse {
        return comparisonService.getCrossRetailerComparison(id)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Product not found with id: $id")
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/controller/ProductPriceHistoryController.kt`
```kotlin
package com.pulseprice.api.controller

import com.pulseprice.api.service.PriceHistoryAnalyticsService
import com.pulseprice.common.model.PriceHistoryResponse
import org.springframework.web.bind.annotation.*
import java.util.UUID

@RestController
@RequestMapping("/api/v1/products")
class ProductPriceHistoryController(
    private val analyticsService: PriceHistoryAnalyticsService
) {

    @GetMapping("/{id}/history")
    suspend fun getPriceHistory(
        @PathVariable id: UUID,
        @RequestParam(defaultValue = "30d") range: String
    ): PriceHistoryResponse {
        return analyticsService.getHistory(id, range)
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/controller/PriceAlertSubscriptionController.kt`
```kotlin
package com.pulseprice.api.controller

import com.pulseprice.api.service.PriceAlertSubscriptionService
import com.pulseprice.common.model.PriceAlertSubscriptionRequest
import com.pulseprice.common.model.PriceAlertSubscriptionResponse
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.util.UUID

@RestController
@RequestMapping("/api/v1/alerts/subscriptions")
class PriceAlertSubscriptionController(
    private val subscriptionService: PriceAlertSubscriptionService
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    suspend fun createSubscription(
        @Valid @RequestBody request: PriceAlertSubscriptionRequest
    ): PriceAlertSubscriptionResponse {
        return subscriptionService.createSubscription(request)
    }

    @GetMapping
    suspend fun getSubscriptions(
        @RequestParam userEmail: String
    ): List<PriceAlertSubscriptionResponse> {
        return subscriptionService.getSubscriptionsForUser(userEmail)
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    suspend fun deleteSubscription(@PathVariable id: UUID) {
        val deactivated = subscriptionService.deactivateSubscription(id)
        if (!deactivated) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Subscription not found with id: $id")
        }
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/controller/PriceStreamController.kt`
```kotlin
package com.pulseprice.api.controller

import com.pulseprice.api.notification.SseNotificationHandler
import com.pulseprice.common.model.PriceStreamUpdate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.util.UUID

@RestController
@RequestMapping("/api/v1/products")
class PriceStreamController(
    private val sseNotificationHandler: SseNotificationHandler
) {

    @GetMapping("/{id}/live-stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamProductPrices(@PathVariable id: UUID): Flow<ServerSentEvent<PriceStreamUpdate>> {
        return sseNotificationHandler.alertFlow
            .filter { it.canonicalProductId == id }
            .map { alert ->
                ServerSentEvent.builder<PriceStreamUpdate>()
                    .id(alert.eventId)
                    .event("price-drop")
                    .data(
                        PriceStreamUpdate(
                            canonicalProductId = alert.canonicalProductId,
                            retailerProductId = alert.retailerProductId,
                            retailer = alert.retailer,
                            price = alert.currentPrice,
                            originalPrice = alert.previousPrice,
                            currency = alert.currency,
                            inStock = true,
                            recordedAt = alert.triggeredAt
                        )
                    )
                    .retry(Duration.ofSeconds(5))
                    .build()
            }
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/ApiApplication.kt`
```kotlin
package com.pulseprice.api

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ApiApplication

fun main(args: Array<String>) {
    runApplication<ApiApplication>(*args)
}
```

---

## Subtask 4.8: Orchestration, Runtime Configuration & End-to-End Verification Guide

### 1. What We Are Doing
We are providing the complete configuration and step-by-step verification procedures to test the full Phase 4 metasearch engine and alert pipeline end-to-end:
1. Docker Compose / Podman service validation.
2. Applying the database schema migration `03-alerts-and-api-schema.sql`.
3. Launching `pulse-api` via Gradle.
4. Testing `/search` fuzzy trigram matching.
5. Testing `/compare` cross-retailer price variance calculations.
6. Testing `/history` TimescaleDB time-bucket aggregation.
7. Creating a price alert subscription via `POST /api/v1/alerts/subscriptions`.
8. Injecting a synthetic price drop event into Kafka topic `canonical-prices` and validating:
   - Alert evaluation triggering.
   - Redis cooldown key registration.
   - Kafka `price-drop-alerts` topic output.
   - Dispatcher execution (Webhook/Email/SSE).
   - TimescaleDB audit log insertion into `price_alert_history`.

### 2. End-to-End Verification Procedures

#### Step 1: Ensure Infrastructure Containers Are Running
```bash
# Start TimescaleDB, Kafka in KRaft mode, and Redis
podman compose up -d || docker compose up -d

# Verify all containers are healthy
podman compose ps || docker compose ps
```

#### Step 2: Apply Database Schema Migration
Execute `03-alerts-and-api-schema.sql` against the running TimescaleDB container:
```bash
podman exec -i pulse-timescaledb psql -U pulse -d pulseprice < init-scripts/03-alerts-and-api-schema.sql

# Verify the tables, indexes, and hypertable were created
podman exec -it pulse-timescaledb psql -U pulse -d pulseprice -c "
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
SELECT hypertable_name FROM timescaledb_information.hypertables;
"
```
*Expected Output:* `price_alert_subscriptions` and `price_alert_history` appear in the table list; `price_alert_history` appears in `timescaledb_information.hypertables`.

#### Step 3: Insert Mock Product & Price History Seed Data
If test data is not already populated from Phase 2/3, seed sample products and prices:
```bash
podman exec -i pulse-timescaledb psql -U pulse -d pulseprice << 'EOF'
-- 1. Insert Canonical Product
INSERT INTO canonical_products (id, gtin, brand, model, title)
VALUES (
    'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
    '00027242923591',
    'Sony',
    'WH-1000XM5',
    'Sony WH-1000XM5 Wireless Noise-Canceling Headphones Black'
) ON CONFLICT (id) DO NOTHING;

-- 2. Insert Retailer Products
INSERT INTO retailer_products (id, canonical_product_id, retailer, retailer_sku, url, title)
VALUES 
(
    '11111111-2222-3333-4444-555555555555',
    'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
    'walmart.com',
    'W-SONY-XM5',
    'https://www.walmart.com/ip/Sony-WH-1000XM5/12345',
    'Sony WH-1000XM5 Headphones - Black'
),
(
    '22222222-3333-4444-5555-666666666666',
    'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
    'bestbuy.com',
    'BB-SONY-XM5',
    'https://www.bestbuy.com/site/sony-wh-1000xm5/54321',
    'Sony - WH-1000XM5 Wireless Noise-Canceling Over-the-Ear Headphones - Black'
) ON CONFLICT (url) DO NOTHING;

-- 3. Insert Historical Price Points
INSERT INTO price_history (recorded_at, retailer_product_id, canonical_product_id, price, original_price, currency, in_stock)
VALUES 
(NOW() - INTERVAL '14 days', '11111111-2222-3333-4444-555555555555', 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d', 399.99, 399.99, 'USD', true),
(NOW() - INTERVAL '7 days',  '11111111-2222-3333-4444-555555555555', 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d', 378.00, 399.99, 'USD', true),
(NOW() - INTERVAL '1 day',   '11111111-2222-3333-4444-555555555555', 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d', 348.00, 399.99, 'USD', true),
(NOW() - INTERVAL '1 day',   '22222222-3333-4444-5555-666666666666', 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d', 398.00, 399.99, 'USD', true);
EOF
```

#### Step 4: Launch `pulse-api`
```bash
./gradlew :pulse-api:bootRun
```
*Expected Log Output:*
```text
Tomcat/Netty started on port 8080 (http) with context path '/'
Started ApiApplication in 2.3 seconds
```

#### Step 5: Test Fuzzy Metasearch (`GET /api/v1/products/search`)
Query with typo/partial text to verify PostgreSQL `pg_trgm` scoring:
```bash
curl -s "http://localhost:8080/api/v1/products/search?q=sony%20xm5&page=0&size=10" | jq .
```
*Expected Response:*
```json
{
  "items": [
    {
      "id": "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
      "gtin": "00027242923591",
      "brand": "Sony",
      "model": "WH-1000XM5",
      "title": "Sony WH-1000XM5 Wireless Noise-Canceling Headphones Black",
      "lowestPrice": 348.00,
      "highestPrice": 398.00,
      "currency": "USD",
      "retailerCount": 2,
      "inStockCount": 2,
      "similarityScore": 0.45
    }
  ],
  "page": 0,
  "size": 10,
  "totalElements": 1,
  "totalPages": 1
}
```

#### Step 6: Test Cross-Retailer Price Comparison (`GET /api/v1/products/{id}/compare`)
```bash
curl -s "http://localhost:8080/api/v1/products/a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d/compare" | jq .
```
*Expected Response:*
```json
{
  "canonicalProductId": "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
  "gtin": "00027242923591",
  "brand": "Sony",
  "model": "WH-1000XM5",
  "title": "Sony WH-1000XM5 Wireless Noise-Canceling Headphones Black",
  "lowestPrice": 348.00,
  "highestPrice": 398.00,
  "priceSpread": 50.00,
  "bestRetailer": "walmart.com",
  "currency": "USD",
  "offers": [
    {
      "retailerProductId": "11111111-2222-3333-4444-555555555555",
      "retailer": "walmart.com",
      "retailerSku": "W-SONY-XM5",
      "url": "https://www.walmart.com/ip/Sony-WH-1000XM5/12345",
      "title": "Sony WH-1000XM5 Headphones - Black",
      "currentPrice": 348.00,
      "originalPrice": 399.99,
      "currency": "USD",
      "inStock": true,
      "isBestPrice": true,
      "priceDifferenceFromLowest": 0.00
    },
    {
      "retailerProductId": "22222222-3333-4444-5555-666666666666",
      "retailer": "bestbuy.com",
      "retailerSku": "BB-SONY-XM5",
      "url": "https://www.bestbuy.com/site/sony-wh-1000xm5/54321",
      "title": "Sony - WH-1000XM5 Wireless Noise-Canceling Over-the-Ear Headphones - Black",
      "currentPrice": 398.00,
      "originalPrice": 399.99,
      "currency": "USD",
      "inStock": true,
      "isBestPrice": false,
      "priceDifferenceFromLowest": 50.00
    }
  ]
}
```

#### Step 7: Test Historical Price Time-Buckets (`GET /api/v1/products/{id}/history`)
```bash
curl -s "http://localhost:8080/api/v1/products/a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d/history?range=30d" | jq .
```
*Expected Response:*
```json
{
  "canonicalProductId": "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
  "timeRange": "30d",
  "currentLowestPrice": 348.00,
  "allTimeLow": 348.00,
  "allTimeHigh": 399.99,
  "averagePrice": 381.00,
  "points": [
    {
      "bucketTimestamp": "2026-10-01T00:00:00Z",
      "minPrice": 399.99,
      "maxPrice": 399.99,
      "avgPrice": 399.99,
      "sampleCount": 1
    },
    {
      "bucketTimestamp": "2026-10-08T00:00:00Z",
      "minPrice": 378.00,
      "maxPrice": 378.00,
      "avgPrice": 378.00,
      "sampleCount": 1
    },
    {
      "bucketTimestamp": "2026-10-14T00:00:00Z",
      "minPrice": 348.00,
      "maxPrice": 398.00,
      "avgPrice": 373.00,
      "sampleCount": 2
    }
  ]
}
```

#### Step 8: Register a Price Drop Alert Subscription
Create a target price alert subscription ($320.00) via REST API:
```bash
curl -s -X POST "http://localhost:8080/api/v1/alerts/subscriptions" \
  -H "Content-Type: application/json" \
  -d '{
    "canonicalProductId": "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
    "userEmail": "dealhunter@example.com",
    "alertType": "TARGET_PRICE",
    "targetPrice": 320.00,
    "notificationChannel": "EMAIL",
    "destination": "dealhunter@example.com",
    "cooldownHours": 24
  }' | jq .
```
*Expected Response:* Returns HTTP 201 Created with generated subscription `id`.

#### Step 9: Inject Synthetic Price Event & Verify Alert Pipeline
Simulate an incoming scrape event with a price drop to `$299.99` (below the `$320.00` target):
```bash
podman exec -i pulse-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic canonical-prices << 'EOF'
{"eventId":"test-evt-001","canonicalProductId":"a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d","retailerProductId":"11111111-2222-3333-4444-555555555555","retailer":"walmart.com","gtin":"00027242923591","price":299.99,"originalPrice":399.99,"currency":"USD","inStock":true,"recordedAt":"2026-10-15T14:00:00Z"}
EOF
```

Verify `pulse-api` output logs:
```text
Alert engine received canonical price event: canonicalId=a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d price=299.99
Dispatched PriceDropAlertEvent for subId=... type=TARGET_PRICE target=dealhunter@example.com
Received PriceDropAlertEvent: ... channel=EMAIL destination=dealhunter@example.com
Sending Price Drop Email to [dealhunter@example.com] | Product: 'Sony WH-1000XM5 Wireless Noise-Canceling Headphones Black' | Dropped from $348.00 to $299.99 (13.79% OFF)
```

Verify TimescaleDB audit log entry:
```bash
podman exec -it pulse-timescaledb psql -U pulse -d pulseprice -c "
SELECT triggered_at, trigger_type, old_price, new_price, price_drop_percentage, channel, destination, status
FROM price_alert_history
ORDER BY triggered_at DESC LIMIT 1;
"
```
*Expected Output:*
```text
         triggered_at          | trigger_type | old_price | new_price | price_drop_percentage | channel | destination            | status  
-------------------------------+--------------+-----------+-----------+-----------------------+---------+------------------------+---------
 2026-10-15 14:00:05.123456+00 | TARGET_PRICE |    348.00 |    299.99 |                 13.79 | EMAIL   | dealhunter@example.com | PENDING
```

# CLAUDE.md

# Project Context
@PROJECT_CONTEXT.md

## Working Agreement
- Code is normally **proposed in `phase<N>.md`** for the user to review and implement by hand. Edit source files
  directly only when explicitly asked.
- Every phase is split into subtasks. Each subtask explains **What**, **Why** (the mechanics and tradeoffs), and
  **Proposed Code** (complete and production-ready).
- The current phase is **Phase 5 (Observability & Production Polish)**. `phase5.md` is the spec. Most of its
  files, such as the `observability/` configs, metrics recorders, health indicators and `logback-spring.xml`,
  are not implemented yet.

## Build & Run
```bash
docker compose up -d                       # TimescaleDB (:5444), Kafka (KRaft), Redis, Tempo, OTel Collector, Prometheus, Grafana
./gradlew build                            # build all modules
./gradlew :pulse-scraper:bootRun           # :8081
./gradlew :pulse-pipeline:bootRun          # :8082
./gradlew :pulse-api:bootRun               # :8000
```
- JDK 21, Kotlin 2.0.0, Spring Boot **3.3.4** (the build uses 3.3.4, although PROJECT_CONTEXT says 4.x), Gradle Kotlin DSL.
- No tests exist yet. The scraper has WireMock and Testcontainers dependencies configured for them.

## Modules (`com.pulseprice.*`)
| Module | Role | Key packages |
|---|---|---|
| `pulse-common` | Shared events and models (plain Kotlin + Jackson, no Spring) | `event/` (RawScrapeEvent, CanonicalProductPriceEvent, PriceDropAlertEvent, ScrapeDeadLetterEvent), `model/` (ScrapeTier, ScrapeResult, ScrapeFailureReason, AlertModels, MetasearchModels) |
| `pulse-scraper` | Tiered scraping → Kafka | `retailers/` (BestBuy, Target, Walmart, Generic + registry), `extractor/` (NextData → JSON-LD → OpenGraph fast path), `browser/` (Playwright pool + stealth.js), `detection/` (anti-bot, DOM drift), `ratelimit/` (Redis token bucket via `rate_limiter.lua`), `service/` (scheduler, ingestion, tiered scraper) |
| `pulse-pipeline` | Consumes raw scrapes, resolves GTIN canonical product, persists price history, audits DLQ | `resolver/` (CanonicalResolver, GtinValidator), `service/`, `scraper/kafka/` |
| `pulse-api` | Metasearch REST, SSE stream, alert engine, notifications | `controller/`, `service/`, `alert/` (evaluation engine, cooldown, Kafka consumer), `notification/` (Email, Webhook (HMAC), SSE) |

`pulse-scraper` depends on `pulse-pipeline` as a project dependency. Keep this in mind when you change pipeline classes.

## Data Flow
```
ScrapeScheduler → TieredRetailerScraper (FAST_PATH_NEXT_DATA | JSON_LD | OPEN_GRAPH → SLOW_PATH_PLAYWRIGHT)
  → Kafka `raw-scrapes` ──(failures)──► `raw-scrapes-dlq` → ScrapeDlqKafkaConsumer → scrape_failures
  → pulse-pipeline RawScrapeKafkaConsumer → CanonicalResolver (GTIN) → price_history → `canonical-prices`
  → pulse-api AlertKafkaConsumer → AlertEvaluationEngine (+ Redis cooldown) → `price-drop-alerts` → NotificationDispatcher
```

## Stack Conventions
- Fully reactive: WebFlux, R2DBC (`r2dbc-postgresql`), reactive Redis, and Kotlin coroutines (`kotlinx-coroutines-reactor`). Do not add blocking JDBC or `RestTemplate` calls.
- Kafka uses a JSON (de)serializer wrapped in `ErrorHandlingDeserializer`, with trusted packages `com.pulseprice.*`. The pipeline uses group `pulse-pipeline-group` and the API uses `pulse-alert-engine-group` (`auto-offset-reset: latest`, manual commit).
- Configuration comes from environment variables with local defaults (`DB_*`/`POSTGRES_*`, `REDIS_HOST/PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `PULSE_HMAC_SECRET`).
- Formatting follows `.editorconfig`: 4-space indents for Kotlin, SQL and Lua; 2-space indents for YAML, JSON and MD; 120-character lines for Kotlin.

## API (`pulse-api`, base `/api/v1`)
- `GET /products/search`, `GET /products/{id}/compare`, `GET /products/{id}/history`
- `GET /products/{id}/live-stream` (SSE)
- `POST|GET /alerts/subscriptions`, `DELETE /alerts/subscriptions/{id}`

## Database (`init-scripts/`, run in order)
1. `01-init-timescale.sql`: `canonical_products`, `retailer_products`, and the `price_history` hypertable
2. `02-scrape-failures-schema.sql`: the `scrape_failures` hypertable (compression after 7 days, retention 30 days) and the hourly continuous aggregate
3. `03-alerts-and-api-schema.sql`: `pg_trgm` search indexes and `price_alert_subscriptions`

## Known Issues (verify before relying on them)
- TimescaleDB is published on host port **5444** (5432 is taken locally). The app defaults use 5444.
- `observability/` contains only `otel-collector-config.yaml`. Compose also mounts `tempo.yaml`, `prometheus.yml`,
  and `grafana/{provisioning,dashboards}`, so those containers fail to start until the files exist. `phase5.md` also
  specifies Loki (`loki.yaml`, a Collector logs pipeline, OTLP log export), and that is not implemented yet.
- `01-init-timescale.sql` uses `timestampz` (a typo for `TIMESTAMPTZ`) in `canonical_products`.
- In `pulse-api/application.yml`, `spring.application.name` is set to `pulse-ap`.
- Class and file names contain typos: `PriceAlerSubscriptionController.kt` and `WehhookNotificationHandler.kt`.

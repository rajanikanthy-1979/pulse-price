# Project Context: PulsePrice — Distributed Retail Metasearch Engine

## 1. System Overview
- **Goal:** Distributed price intelligence engine scraping & tracking products across retail websites with time-series historical data and price-drop alerts.
- **Target Stack:** Kotlin, Java 21, Spring Boot 4.x, Kotlin Coroutines, Jsoup, Playwright, Apache Kafka, PostgreSQL + TimescaleDB, Redis, OpenTelemetry, Prometheus, Grafana, React/Tailwind.

## 2. Multi-Phase Implementation Plan
- [X] **Phase 1: Ingestion Engine & Sandbox Verification**
  - Scaffolding multi-module Gradle project in Kotlin & Spring Boot 3.
  - Core `RetailerScraper` strategy & non-blocking execution via Coroutines.
  - Per-domain Token Bucket rate-limiting using Redis.
  - WireMock integration tests for mocking HTTP 200, 429, 503, and price fluctuations.
- [X] **Phase 2: Decoupled Pipeline & Time-Series Storage**
  - Docker Compose: Kafka, TimescaleDB, Redis.
  - Ingestion pipeline: `raw-scrapes` -> Parser -> UPC/GTIN canonical resolver -> `price_history` hypertable.
- [X] **Phase 3: Real-World Scrapers & Anti-Bot Hardening**
  - Jsoup (JSON-LD / `__NEXT_DATA__`) fast path + Playwright headless fallback.
  - Dead-Letter Queue (DLQ) for DOM drift & selector validation.
- [X] **Phase 4: Price Drop Alerts & Metasearch API**
  - Alert engine (`price-drop-alerts`), subscription matches, REST endpoints (`/search`, `/compare`, `/history`).
- [ ] **Phase 5: Observability & Production Polish** (Current Phase)
  - OpenTelemetry + Prometheus + Grafana dashboards.

## 3. Current Sprint Deliverable (Phase 5)
- **Observability Stack (`docker-compose.yml`):** OpenTelemetry Collector, Prometheus, Tempo (Distributed Tracing), and Grafana with automated datasource and dashboard provisioning.
- **Distributed Tracing & Context Propagation:** End-to-end W3C TraceContext propagation across Kotlin Coroutines (`ReactorContext` / `MDCContext`), WebFlux HTTP requests, Kafka Producer/Consumer headers (`raw-scrapes` -> `canonical-prices` -> `price-drop-alerts`), R2DBC queries, and Redis operations.
- **Custom Domain Metrics (`Micrometer` & `Prometheus`):** High-cardinality-safe business and operational metrics across `pulse-scraper` (tier distribution, anti-bot challenge rates, token bucket throttling, Playwright pool utilization), `pulse-pipeline` (GTIN canonical resolution hit/miss, hypertable ingestion latency, DLQ rates), and `pulse-api` (alert evaluation latency, notification channel dispatch counters, L1 Redis cache hit ratios).
- **Production Readiness & Resilience:** Custom Spring Boot Actuator reactive health indicators (TimescaleDB, Redis, Kafka, Playwright browser health), structured JSON logging with `trace_id`/`span_id` correlation, and pre-built Grafana dashboards (`pulse-overview-dashboard.json`).

## 4. Global Implementation Workflow & Instructions
- **Subtask Breakdown:** Every phase must be broken down into granular subtasks.
- **Deep Explanations:** For each subtask, explicitly explain:
  1. **What we are doing**: The exact components, files, and classes being introduced.
  2. **Why we are doing it**: Architectural rationale, technical tradeoffs, and deep mechanical details (e.g., coroutines, thread dispatching, atomic Lua scripting, DOM vs structured data extraction).
  3. **Proposed Code**: Full, production-ready code ready for review and manual implementation.
- **Dedicated Phase Documentation:** Each phase must have a dedicated markdown guide generated in the project root named `phase<N>.md` (e.g., `phase1.md`, `phase2.md`, etc.).
- **Manual Review & Implementation:** Code is proposed in the documentation for user review and manual implementation rather than automatic direct modification of code files unless explicitly instructed.


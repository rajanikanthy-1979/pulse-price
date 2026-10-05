# Project Context: PulsePrice — Distributed Retail Metasearch Engine

## 1. System Overview
- **Goal:** Distributed price intelligence engine scraping & tracking products across retail websites with time-series historical data and price-drop alerts.
- **Target Stack:** Kotlin, Java 21, Spring Boot 4.x, Kotlin Coroutines, Jsoup, Playwright, Apache Kafka, PostgreSQL + TimescaleDB, Redis, OpenTelemetry, Prometheus, Grafana, React/Tailwind.

## 2. Multi-Phase Implementation Plan
- [X] **Phase 1: Ingestion Engine & Sandbox Verification** (Current Phase)
  - Scaffolding multi-module Gradle project in Kotlin & Spring Boot 3.
  - Core `RetailerScraper` strategy & non-blocking execution via Coroutines.
  - Per-domain Token Bucket rate-limiting using Redis.
  - WireMock integration tests for mocking HTTP 200, 429, 503, and price fluctuations.
- [ ] **Phase 2: Decoupled Pipeline & Time-Series Storage**
  - Docker Compose: Kafka, TimescaleDB, Redis.
  - Ingestion pipeline: `raw-scrapes` -> Parser -> UPC/GTIN canonical resolver -> `price_history` hypertable.
- [ ] **Phase 3: Real-World Scrapers & Anti-Bot Hardening**
  - Jsoup (JSON-LD / `__NEXT_DATA__`) fast path + Playwright headless fallback.
  - Dead-Letter Queue (DLQ) for DOM drift & selector validation.
- [ ] **Phase 4: Price Drop Alerts & Metasearch API**
  - Alert engine (`price-drop-alerts`), subscription matches, REST endpoints (`/search`, `/compare`, `/history`).
- [ ] **Phase 5: Observability & Production Polish**
  - OpenTelemetry + Prometheus + Grafana dashboards.

## 3. Current Sprint Deliverable (Phase 1)
- Working multi-module build file (`build.gradle.kts`).
- `RetailerScraper` interface & Jsoup/JSON-LD extractor.
- Redis Token Bucket Rate Limiter.
- WireMock automated integration tests validating non-blocking scraping and rate limits.

## 4. Global Implementation Workflow & Instructions
- **Subtask Breakdown:** Every phase must be broken down into granular subtasks.
- **Deep Explanations:** For each subtask, explicitly explain:
  1. **What we are doing**: The exact components, files, and classes being introduced.
  2. **Why we are doing it**: Architectural rationale, technical tradeoffs, and deep mechanical details (e.g., coroutines, thread dispatching, atomic Lua scripting, DOM vs structured data extraction).
  3. **Proposed Code**: Full, production-ready code ready for review and manual implementation.
- **Dedicated Phase Documentation:** Each phase must have a dedicated markdown guide generated in the project root named `phase<N>.md` (e.g., `phase1.md`, `phase2.md`, etc.).
- **Manual Review & Implementation:** Code is proposed in the documentation for user review and manual implementation rather than automatic direct modification of code files unless explicitly instructed.


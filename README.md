# PulsePrice

> Distributed Retail Metasearch Engine — scraping & tracking products across retail websites with time-series historical data and price-drop alerts.

---

## 🛠 Tech Stack

- **Language & Runtime:** Kotlin 2.0, Java 21
- **Framework:** Spring Boot 3.3.4, Spring WebFlux, Kotlin Coroutines
- **Data & Ingestion:** Jsoup, Jackson Kotlin Module
- **Event Streaming & Cache:** Apache Kafka, Redis 7 (Reactive & Token Bucket rate limiting via Lua)
- **Time-Series Storage:** PostgreSQL 16 + TimescaleDB
- **Build System:** Gradle (Kotlin DSL)

---

## 📁 Repository Structure

```text
pulse-price/
├── gradle/wrapper/                 # Gradle wrapper binary and configuration
├── gradlew, gradlew.bat            # Gradle wrapper execution scripts
├── build.gradle.kts                # Root multi-module build configuration
├── settings.gradle.kts             # Subproject declarations
├── docker-compose.yml              # Local infrastructure (TimescaleDB, Kafka, Redis)
├── init-scripts/                   # Database bootstrap SQL (TimescaleDB hypertables)
├── pulse-common/                   # Shared domain models & sealed scrape contracts
│   └── src/main/kotlin/...
├── pulse-scraper/                  # Ingestion engine & per-domain rate limiter
│   └── src/main/kotlin/...
├── PROJECT_CONTEXT.md              # Global project architecture & roadmap
├── phase1.md                       # Phase 1: Ingestion Engine & Sandbox Verification
└── phase2.md                       # Phase 2: Decoupled Pipeline & Time-Series Storage
```

---

## 🚀 Quick Start

### 1. Prerequisites
- **JDK 21** installed and configured (`JAVA_HOME`)
- **Docker** and **Docker Compose** installed and running

### 2. Start Infrastructure
Start TimescaleDB, Kafka, and Redis services:
```bash
docker compose up -d
```

### 3. Build the Project
Build all modules and verify tests:
```bash
./gradlew build
```

### 4. Run the Scraper Service
```bash
./gradlew :pulse-scraper:bootRun
```

---

## 📖 Documentation
- [PROJECT_CONTEXT.md](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/PROJECT_CONTEXT.md): System architecture, phases, and global guidelines.
- [phase1.md](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/phase1.md): Deep-dive into ingestion architecture, Coroutines, rate limiting, and extraction mechanics.
- [phase2.md](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/phase2.md): Decoupled Kafka pipeline and TimescaleDB hypertable design.

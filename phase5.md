# Phase 5: Observability & Production Polish

This document provides a comprehensive, production-grade implementation guide for **Phase 5: Observability & Production Polish**. It equips the distributed PulsePrice metasearch engine (`pulse-scraper`, `pulse-pipeline`, `pulse-api`) with end-to-end distributed tracing via **OpenTelemetry & Grafana Tempo**, low-cardinality business and operational telemetry via **Micrometer & Prometheus**, reactive infrastructure health checks via **Spring Boot Actuator**, structured JSON log correlation, and an automated **Grafana** observability dashboard.

Infrastructure is orchestrated using **Podman Compose** (`podman compose`) / **Docker Compose** (`docker compose`). In accordance with project instructions, all code and configurations are proposed in full for manual review and implementation.

---

## Architecture & Directory Layout

Phase 5 introduces the **Unified Observability & Production Readiness Subsystem**:
1. **End-to-End W3C Trace Context Propagation:** Correlates a single price scrape from `pulse-scraper` (HTTP WebClient / Playwright fallback) $\rightarrow$ Kafka `raw-scrapes` $\rightarrow$ `pulse-pipeline` (Redis GTIN resolution + TimescaleDB R2DBC insert) $\rightarrow$ Kafka `canonical-prices` $\rightarrow$ `pulse-api` (Alert evaluation + Webhook/Email/SSE notification dispatch) under a **single distributed `traceId`**.
2. **Coroutine-Safe Context Bridging:** Bridges Project Reactor's `ContextView`, SLF4J `MDC`, and Micrometer `ObservationRegistry` across Kotlin Coroutine suspension points (`ReactorContext` + `ObservationContextElement`) so threads switching between Netty event loops, Kafka consumer pools, and `Dispatchers.IO` never lose active trace spans.
3. **High-Signal Custom Domain Metrics:** Exposes Prometheus counters, timers, and gauges for scraper tier distribution, anti-bot challenge detection rates, Redis token bucket throttling, GTIN canonical resolution cache hit ratios, TimescaleDB hypertable write latency, and alert channel delivery rates.
4. **Zero-Touch Observability Stack:** Adds OpenTelemetry Collector, Prometheus, Grafana Tempo, and Grafana with pre-provisioned datasources and a complete 12-panel operational dashboard JSON.

```text
pulse-price/
├── docker-compose.yml
├── observability/
│   ├── otel-collector-config.yaml
│   ├── prometheus.yml
│   ├── tempo.yaml
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/
│       │   │   └── datasources.yaml
│       │   └── dashboards/
│       │       └── dashboards.yaml
│       └── dashboards/
│           └── pulse-overview-dashboard.json
├── build.gradle.kts
├── pulse-common/
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/pulseprice/common/
│       └── observability/
│           ├── CoroutineTraceContext.kt
│           ├── PulseMetricNames.kt
│           └── KafkaObservationHelper.kt
├── pulse-scraper/
│   ├── build.gradle.kts
│   └── src/main/
│       ├── kotlin/com/pulseprice/scraper/
│       │   ├── config/
│       │   │   └── ScraperObservabilityConfig.kt
│       │   ├── health/
│       │   │   └── PlaywrightBrowserHealthIndicator.kt
│       │   ├── metrics/
│       │   │   └── ScraperMetricsRecorder.kt
│       │   └── service/
│       │       ├── TieredRetailerScraper.kt      (Instrumented)
│       │       └── ScrapeIngestionService.kt     (Instrumented)
│       └── resources/
│           ├── application.yml
│           └── logback-spring.xml
├── pulse-pipeline/
│   ├── build.gradle.kts
│   └── src/main/
│       ├── kotlin/com/pulseprice/pipeline/
│       │   ├── config/
│       │   │   ├── KafkaConsumerConfig.kt        (Observation enabled)
│       │   │   └── KafkaProducerConfig.kt        (Observation enabled)
│       │   ├── metrics/
│       │   │   └── PipelineMetricsRecorder.kt
│       │   ├── resolver/
│       │   │   └── CanonicalResolver.kt          (Instrumented)
│       │   ├── scraper/kafka/
│       │   │   ├── RawScrapeKafkaConsumer.kt     (Trace propagation)
│       │   │   └── ScrapeDlqKafkaConsumer.kt     (Instrumented)
│       │   └── service/
│       │       └── PricePersistenceService.kt    (Instrumented)
│       └── resources/
│           ├── application.yml
│           └── logback-spring.xml
└── pulse-api/
    ├── build.gradle.kts
    └── src/main/
        ├── kotlin/com/pulseprice/api/
        │   ├── config/
        │   │   ├── ApiObservabilityConfig.kt
        │   │   └── KafkaApiConfig.kt             (Observation enabled)
        │   ├── health/
        │   │   └── PulseInfrastructureHealthIndicator.kt
        │   ├── metrics/
        │   │   └── ApiMetricsRecorder.kt
        │   ├── alert/
        │   │   ├── AlertEvaluationEngine.kt      (Instrumented)
        │   │   └── AlertKafkaConsumer.kt         (Trace propagation)
        │   └── notification/
        │       └── NotificationDispatcher.kt     (Instrumented)
        └── resources/
            ├── application.yml
            └── logback-spring.xml
```

---

## Subtask 5.1: Observability Infrastructure Stack (`docker-compose.yml`, OTel Collector, Prometheus, Tempo, Grafana)

### 1. What We Are Doing
We are extending `docker-compose.yml` with four production observability containers and their declarative configuration files under `observability/`:
1. **OpenTelemetry Collector (`otel/opentelemetry-collector-contrib`):** Receives OTLP (gRPC `4317` / HTTP `4318`) traces and metrics from all three Spring Boot services, batches them, and exports traces to Tempo and metrics to Prometheus.
2. **Grafana Tempo (`grafana/tempo`):** High-scale, object-storage-friendly distributed tracing backend that indexes trace IDs and stores spans without requiring a heavy Elasticsearch cluster.
3. **Prometheus (`prom/prometheus`):** Scrapes `/actuator/prometheus` endpoints across `pulse-scraper:8081`, `pulse-pipeline:8082`, and `pulse-api:8000` (or `8080`), as well as the OTel Collector metrics exporter.
4. **Grafana (`grafana/grafana`):** Pre-provisioned with Prometheus and Tempo datasources (including Trace-to-Logs and Exemplar linking) and a ready-to-use `pulse-overview-dashboard.json`.

### 2. Why We Are Doing It & Deep Mechanics

#### Why Decouple Services from Backends with the OpenTelemetry Collector?
- **Vendor-Agnostic Instrumentation:** If application services export spans directly to Jaeger, Zipkin, or Datadog, changing backends requires recompiling and redeploying every microservice. By speaking standard **OTLP (OpenTelemetry Protocol)** to a local collector agent, microservices remain 100% decoupled from storage backends.
- **Batching & Tail-Sampling Offloading:** Emitting spans synchronously over HTTP on every scrape or Kafka message introduces network overhead. The OTel Collector `batch` processor buffers spans in memory and compresses network flushes, while `memory_limiter` protects the collector against OOM spikes during scraping bursts.
- **Prometheus Exemplars:** When Micrometer records a latency histogram (e.g., `pulse_scraper_execution_duration_seconds`), it attaches the active `trace_id` as a Prometheus **Exemplar**. In Grafana, engineers can click a high-latency p99 spike directly on a Prometheus graph and jump straight to the exact distributed trace in Tempo.

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
      - "5444:5432"
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
      timeout: 3s
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

  # =========================================================================
  # Phase 5: Observability Stack (OTel Collector, Tempo, Prometheus, Grafana)
  # =========================================================================

  tempo:
    image: grafana/tempo:2.6.0
    container_name: pulse-tempo
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./observability/tempo.yaml:/etc/tempo.yaml:z
      - tempo_data:/var/tempo
    ports:
      - "3200:3200"   # Tempo HTTP query frontend

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.111.0
    container_name: pulse-otel-collector
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./observability/otel-collector-config.yaml:/etc/otel-collector-config.yaml:z
    ports:
      - "4317:4317"   # OTLP gRPC receiver
      - "4318:4318"   # OTLP HTTP receiver
      - "8889:8889"   # Prometheus exporter metrics
    depends_on:
      - tempo

  prometheus:
    image: prom/prometheus:v2.54.1
    container_name: pulse-prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"
      - "--enable-feature=exemplar-storage"
      - "--web.enable-remote-write-receiver"
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml:z
      - prometheus_data:/prometheus
    extra_hosts:
      - "host.docker.internal:host-gateway"
      - "host.containers.internal:host-gateway"
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:11.2.2
    container_name: pulse-grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning:z
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards:z
      - grafana_data:/var/lib/grafana
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
      - tempo

volumes:
  timescaledb_data:
  tempo_data:
  prometheus_data:
  grafana_data:
```

#### File: `observability/otel-collector-config.yaml`
```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
    spike_limit_mib: 128
  batch:
    send_batch_size: 512
    timeout: 2s

exporters:
  otlp/tempo:
    endpoint: tempo:4317
    tls:
      insecure: true
  prometheus:
    endpoint: "0.0.0.0:8889"
    namespace: pulse_otel
  debug:
    verbosity: basic

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/tempo, debug]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheus]
```

#### File: `observability/tempo.yaml`
```yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318

ingester:
  max_block_duration: 5m

compactor:
  compaction:
    block_retention: 24h

storage:
  trace:
    backend: local
    wal:
      path: /var/tempo/wal
    local:
      path: /var/tempo/blocks
```

#### File: `observability/prometheus.yml`
```yaml
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
  - job_name: "pulse-scraper"
    metrics_path: "/actuator/prometheus"
    static_configs:
      - targets: ["host.containers.internal:8081", "host.docker.internal:8081"]
        labels:
          service: "pulse-scraper"

  - job_name: "pulse-pipeline"
    metrics_path: "/actuator/prometheus"
    static_configs:
      - targets: ["host.containers.internal:8082", "host.docker.internal:8082"]
        labels:
          service: "pulse-pipeline"

  - job_name: "pulse-api"
    metrics_path: "/actuator/prometheus"
    static_configs:
      - targets: ["host.containers.internal:8000", "host.docker.internal:8000", "host.containers.internal:8080", "host.docker.internal:8080"]
        labels:
          service: "pulse-api"

  - job_name: "otel-collector"
    static_configs:
      - targets: ["otel-collector:8889"]
```

#### File: `observability/grafana/provisioning/datasources/datasources.yaml`
```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    jsonData:
      httpMethod: POST
      exemplarTraceIdDestinations:
        - name: trace_id
          datasourceUid: tempo

  - name: Tempo
    type: tempo
    uid: tempo
    access: proxy
    url: http://tempo:3200
    isDefault: false
    jsonData:
      tracesToMetrics:
        datasourceUid: prometheus
```

#### File: `observability/grafana/provisioning/dashboards/dashboards.yaml`
```yaml
apiVersion: 1

providers:
  - name: "PulsePrice Dashboards"
    orgId: 1
    folder: "PulsePrice"
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /var/lib/grafana/dashboards
```

---

## Subtask 5.2: Gradle Dependencies & Coroutine Trace Propagation (`pulse-common` & Root Build)

### 1. What We Are Doing
We are adding **Spring Boot Actuator**, **Micrometer Tracing Bridge for OpenTelemetry (`micrometer-tracing-bridge-otel`)**, **OTLP Trace Exporter (`opentelemetry-exporter-otlp`)**, **Micrometer Prometheus Registry (`micrometer-registry-prometheus`)**, and **Logstash Logback Encoder** across our Gradle build configuration.
In `pulse-common`, we are creating:
- `PulseMetricNames.kt`: Centralized constants for all metric names and tag keys to enforce low-cardinality naming conventions across modules.
- `CoroutineTraceContext.kt`: Utility functions that bridge Micrometer `Observation` / Reactor Context into Kotlin `CoroutineContext` elements (`ReactorContext` + `MDCContext`), ensuring child `launch` or `withContext(Dispatchers.IO)` blocks preserve active `traceId` and `spanId` values.

### 2. Why We Are Doing It & Deep Mechanics

#### The Kotlin Coroutine Thread-Hopping Trap in Distributed Tracing
In traditional synchronous JVM apps, OpenTelemetry and SLF4J store the current `Span` and `MDC` inside `ThreadLocal` variables.
In a reactive Kotlin Coroutines + WebFlux + Kafka architecture:
1. A coroutine starts executing on `reactor-http-nio-2`.
2. It suspends at `webClient.get()...awaitExchange()` or `withContext(Dispatchers.IO)` (for Jsoup parsing or Playwright rendering).
3. When it resumes, Kotlin dispatches the continuation onto `DefaultDispatcher-worker-4`.
4. Without explicit context propagation, `DefaultDispatcher-worker-4` has an empty `ThreadLocal`. Any spans created or logs written after suspension lose their parent `traceId` and appear as orphaned root traces!

To solve this deterministically:
- We include `org.jetbrains.kotlinx:kotlinx-coroutines-slf4j` (`MDCContext()`) and `io.micrometer:context-propagation`.
- We provide `withTracedCoroutine` in `CoroutineTraceContext.kt`, which captures the current Micrometer `ObservationRegistry` / `Tracer` span and restores it across thread hops.

#### Cardinality Protection in Prometheus
Prometheus creates a distinct time-series index entry for every unique combination of metric label values. Passing unbounded variables like `url`, `product_id`, or `user_email` as metric tags causes a **Cardinality Explosion**, exhausting Prometheus RAM within hours. `PulseMetricNames` restricts tags strictly to bounded enums and low-cardinality dimensions: `retailer`, `tier`, `reason`, `channel`, `status`, and `cache_result`.

### 3. Proposed Code

#### File: [`build.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/build.gradle.kts)
```kotlin
plugins {
    kotlin("jvm") version "2.0.0" apply false
    kotlin("plugin.spring") version "2.0.0" apply false
    id("org.springframework.boot") version "3.3.4" apply false
    id("io.spring.dependency-management") version "1.1.6" apply false
}

allprojects {
    group = "com.pulseprice"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "io.spring.dependency-management")

    the<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension>().apply {
        imports {
            mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
        }
    }

    dependencies {
        val implementation by configurations
        val testImplementation by configurations

        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlin:kotlin-stdlib")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.8.1")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:1.8.1")

        // Phase 5: Unified Observability (Micrometer + OpenTelemetry + Prometheus + Structured Logging)
        implementation("io.micrometer:micrometer-core")
        implementation("io.micrometer:micrometer-registry-prometheus")
        implementation("io.micrometer:micrometer-tracing-bridge-otel")
        implementation("io.opentelemetry:opentelemetry-exporter-otlp")
        implementation("io.micrometer:context-propagation")
        implementation("net.logstash.logback:logstash-logback-encoder:7.4")

        testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
            freeCompilerArgs.add("-Xjsr305=strict")
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
```

#### Update Subproject Build Files (`pulse-scraper/build.gradle.kts` & `pulse-pipeline/build.gradle.kts`)
Add `spring-boot-starter-actuator` to both `pulse-scraper/build.gradle.kts` and `pulse-pipeline/build.gradle.kts` (`pulse-api/build.gradle.kts` already includes it):
```kotlin
    // Actuator & Micrometer for Operational Health & Prometheus Metrics
    implementation("org.springframework.boot:spring-boot-starter-actuator")
```

#### File: `pulse-common/src/main/kotlin/com/pulseprice/common/observability/PulseMetricNames.kt`
```kotlin
package com.pulseprice.common.observability

object PulseMetricNames {
    // Scraper Metrics
    const val SCRAPE_EXECUTION_TOTAL = "pulse.scraper.executions.total"
    const val SCRAPE_DURATION_SECONDS = "pulse.scraper.execution.duration"
    const val SCRAPE_ANTIBOT_DETECTED_TOTAL = "pulse.scraper.antibot.detected.total"
    const val SCRAPE_RATE_LIMITED_TOTAL = "pulse.scraper.ratelimited.total"
    const val SCRAPE_DLQ_PUBLISHED_TOTAL = "pulse.scraper.dlq.published.total"
    const val PLAYWRIGHT_ACTIVE_PAGES = "pulse.scraper.playwright.active.pages"

    // Pipeline Metrics
    const val PIPELINE_EVENTS_PROCESSED_TOTAL = "pulse.pipeline.events.processed.total"
    const val PIPELINE_INGESTION_DURATION_SECONDS = "pulse.pipeline.ingestion.duration"
    const val PIPELINE_CANONICAL_RESOLUTION_TOTAL = "pulse.pipeline.canonical.resolution.total"
    const val PIPELINE_DLQ_AUDITED_TOTAL = "pulse.pipeline.dlq.audited.total"

    // API & Alert Engine Metrics
    const val ALERT_EVALUATIONS_TOTAL = "pulse.api.alerts.evaluated.total"
    const val ALERT_TRIGGERS_TOTAL = "pulse.api.alerts.triggered.total"
    const val ALERT_COOLDOWN_SUPPRESSED_TOTAL = "pulse.api.alerts.cooldown.suppressed.total"
    const val NOTIFICATION_DISPATCH_TOTAL = "pulse.api.notifications.dispatched.total"
    const val NOTIFICATION_DURATION_SECONDS = "pulse.api.notifications.duration"
    const val METASEARCH_CACHE_REQUESTS_TOTAL = "pulse.api.cache.requests.total"
    const val SSE_ACTIVE_SUBSCRIBERS = "pulse.api.sse.active.subscribers"

    // Low-Cardinality Tag Keys
    object Tags {
        const val RETAILER = "retailer"
        const val TIER = "tier"
        const val OUTCOME = "outcome"
        const val FAILURE_REASON = "failure_reason"
        const val RESOLUTION_SOURCE = "resolution_source"
        const val ALERT_TYPE = "alert_type"
        const val CHANNEL = "channel"
        const val CACHE_NAME = "cache_name"
        const val CACHE_RESULT = "cache_result"
    }
}
```

#### File: `pulse-common/src/main/kotlin/com/pulseprice/common/observability/CoroutineTraceContext.kt`
```kotlin
package com.pulseprice.common.observability

import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor
import io.micrometer.tracing.Tracer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC
import reactor.util.context.Context
import kotlinx.coroutines.reactor.ReactorContext

/**
 * Executes a suspending block within a child Micrometer Observation span,
 * propagating both the Reactor Observation context and SLF4J MDC (traceId/spanId)
 * across Kotlin Coroutine suspension points and thread hops.
 */
suspend fun <T> ObservationRegistry.observeSuspend(
    spanName: String,
    tracer: Tracer? = null,
    lowCardinalityTags: Map<String, String> = emptyMap(),
    highCardinalityTags: Map<String, String> = emptyMap(),
    block: suspend CoroutineScope.(Observation) -> T
): T {
    val observation = Observation.createNotStarted(spanName, this)
    lowCardinalityTags.forEach { (k, v) -> observation.lowCardinalityKeyValue(k, v) }
    highCardinalityTags.forEach { (k, v) -> observation.highCardinalityKeyValue(k, v) }

    observation.start()
    return try {
        val scope = observation.openScope()
        val mdcMap = try {
            tracer?.currentSpan()?.context()?.let { ctx ->
                MDC.put("traceId", ctx.traceId())
                MDC.put("spanId", ctx.spanId())
            }
            MDC.getCopyOfContextMap() ?: emptyMap()
        } finally {
            scope.close()
        }

        val reactorContext = Context.of(ObservationThreadLocalAccessor.KEY, observation)
        withContext(ReactorContext(reactorContext) + MDCContext(mdcMap)) {
            block(observation)
        }
    } catch (ex: Throwable) {
        observation.error(ex)
        throw ex
    } finally {
        observation.stop()
    }
}
```

---

## Subtask 5.3: Scraper Instrumentation, Metrics & Playwright Health Indicator (`pulse-scraper`)

### 1. What We Are Doing
We are instrumenting `pulse-scraper` with:
1. **`ScraperMetricsRecorder.kt`:** Wraps `MeterRegistry` to record execution counters per retailer and tier (`FAST_PATH_NEXT_DATA`, `FAST_PATH_JSON_LD`, `SLOW_PATH_PLAYWRIGHT`), scrape duration timers, anti-bot challenge detections, Redis rate-limit rejections, DLQ dispatches, and an atomic gauge tracking active headless Chromium pages.
2. **`PlaywrightBrowserHealthIndicator.kt`:** Implements Spring Boot Actuator's `ReactiveHealthIndicator` to verify that the underlying Chromium process is alive (`browser.isConnected`) and reports the browser version and active context count in `/actuator/health`.
3. **`ScraperObservabilityConfig.kt`:** Enables `@Observed` aspect support and configures WebClient auto-instrumentation so outgoing scrape requests generate child HTTP client spans.
4. **Instrumented `TieredRetailerScraper.kt` & `ScrapeIngestionService.kt`:** Integrates `ScraperMetricsRecorder` and `ObservationRegistry` spans around fast-path and slow-path execution.

### 2. Why We Are Doing It & Deep Mechanics

#### Detecting Headless Browser Zombie States & Tier Escalation Trends
- Headless Chromium processes can crash silently due to shared memory (`/dev/shm`) exhaustion or GPU driver faults. Without `PlaywrightBrowserHealthIndicator`, Kubernetes or container orchestrators continue routing scrape jobs to a dead container where 100% of Tier 2 fallbacks fail.
- Tracking the ratio of `FAST_PATH_*` vs `SLOW_PATH_PLAYWRIGHT` executions (`pulse_scraper_executions_total`) provides an immediate early-warning indicator: if a retailer suddenly shifts from 95% Fast-Path to 90% Slow-Path, their edge CDN has tightened TLS/WAF rules or removed SSR hydration tags.

### 3. Proposed Code

#### File: `pulse-scraper/src/main/kotlin/com/pulseprice/scraper/metrics/ScraperMetricsRecorder.kt`
```kotlin
package com.pulseprice.scraper.metrics

import com.pulseprice.common.model.ScrapeFailureReason
import com.pulseprice.common.model.ScrapeTier
import com.pulseprice.common.observability.PulseMetricNames
import com.pulseprice.common.observability.PulseMetricNames.Tags
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

@Component
class ScraperMetricsRecorder(private val meterRegistry: MeterRegistry) {

    private val activePlaywrightPages = AtomicInteger(0)

    init {
        Gauge.builder(PulseMetricNames.PLAYWRIGHT_ACTIVE_PAGES, activePlaywrightPages) { it.get().toDouble() }
            .description("Currently active headless Chromium pages rendering in Playwright")
            .register(meterRegistry)
    }

    fun recordScrapeSuccess(domain: String, tier: ScrapeTier, latencyMs: Long) {
        Counter.builder(PulseMetricNames.SCRAPE_EXECUTION_TOTAL)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .tag(Tags.TIER, tier.name)
            .tag(Tags.OUTCOME, "SUCCESS")
            .register(meterRegistry)
            .increment()

        Timer.builder(PulseMetricNames.SCRAPE_DURATION_SECONDS)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .tag(Tags.TIER, tier.name)
            .tag(Tags.OUTCOME, "SUCCESS")
            .publishPercentileHistogram(true)
            .register(meterRegistry)
            .record(Duration.ofMillis(latencyMs))
    }

    fun recordScrapeFailure(
        domain: String,
        tier: ScrapeTier,
        reason: ScrapeFailureReason,
        latencyMs: Long
    ) {
        Counter.builder(PulseMetricNames.SCRAPE_EXECUTION_TOTAL)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .tag(Tags.TIER, tier.name)
            .tag(Tags.OUTCOME, "FAILURE")
            .tag(Tags.FAILURE_REASON, reason.name)
            .register(meterRegistry)
            .increment()

        Timer.builder(PulseMetricNames.SCRAPE_DURATION_SECONDS)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .tag(Tags.TIER, tier.name)
            .tag(Tags.OUTCOME, "FAILURE")
            .publishPercentileHistogram(true)
            .register(meterRegistry)
            .record(Duration.ofMillis(latencyMs))
    }

    fun recordAntiBotDetected(domain: String, statusCode: Int) {
        Counter.builder(PulseMetricNames.SCRAPE_ANTIBOT_DETECTED_TOTAL)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .tag("http_status", statusCode.toString())
            .register(meterRegistry)
            .increment()
    }

    fun recordRateLimited(domain: String) {
        Counter.builder(PulseMetricNames.SCRAPE_RATE_LIMITED_TOTAL)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .register(meterRegistry)
            .increment()
    }

    fun recordDlqPublished(domain: String, reason: ScrapeFailureReason) {
        Counter.builder(PulseMetricNames.SCRAPE_DLQ_PUBLISHED_TOTAL)
            .tag(Tags.RETAILER, sanitizeDomain(domain))
            .tag(Tags.FAILURE_REASON, reason.name)
            .register(meterRegistry)
            .increment()
    }

    fun incrementPlaywrightActivePages(): Int = activePlaywrightPages.incrementAndGet()
    fun decrementPlaywrightActivePages(): Int = activePlaywrightPages.decrementAndGet()

    private fun sanitizeDomain(domain: String): String =
        domain.lowercase().removePrefix("www.").ifBlank { "unknown" }
}
```

#### File: `pulse-scraper/src/main/kotlin/com/pulseprice/scraper/health/PlaywrightBrowserHealthIndicator.kt`
```kotlin
package com.pulseprice.scraper.health

import com.pulseprice.scraper.browser.PlaywrightBrowserManager
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component("playwrightBrowser")
class PlaywrightBrowserHealthIndicator(
    private val browserManager: PlaywrightBrowserManager
) : ReactiveHealthIndicator {

    override fun health(): Mono<Health> = Mono.fromCallable {
        try {
            val browser = browserManager.getBrowser()
            if (browser.isConnected) {
                Health.up()
                    .withDetail("engine", "chromium")
                    .withDetail("version", browser.version())
                    .withDetail("activeContexts", browser.contexts().size)
                    .build()
            } else {
                Health.down()
                    .withDetail("reason", "Chromium browser process is disconnected")
                    .build()
            }
        } catch (ex: Exception) {
            Health.down(ex).build()
        }
    }
}
```

#### File: `pulse-scraper/src/main/kotlin/com/pulseprice/scraper/config/ScraperObservabilityConfig.kt`
```kotlin
package com.pulseprice.scraper.config

import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.aop.ObservedAspect
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ScraperObservabilityConfig {

    @Bean
    fun observedAspect(observationRegistry: ObservationRegistry): ObservedAspect {
        return ObservedAspect(observationRegistry)
    }
}
```

#### Updated File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/service/ScrapeIngestionService.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/service/ScrapeIngestionService.kt)
```kotlin
package com.pulseprice.scraper.service

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.common.event.ScrapeDeadLetterEvent
import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.common.model.ScrapeTier
import com.pulseprice.common.observability.observeSuspend
import com.pulseprice.pipeline.scraper.kafka.ScrapeEventProducer
import com.pulseprice.scraper.kafka.ScrapeDlqProducer
import com.pulseprice.scraper.metrics.ScraperMetricsRecorder
import com.pulseprice.scraper.retailers.RetailerScraperRegistry
import io.micrometer.observation.ObservationRegistry
import io.micrometer.tracing.Tracer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI

@Service
class ScrapeIngestionService(
    private val scraperRegistry: RetailerScraperRegistry,
    private val eventProducer: ScrapeEventProducer,
    private val dlqProducer: ScrapeDlqProducer,
    private val metricsRecorder: ScraperMetricsRecorder,
    private val observationRegistry: ObservationRegistry,
    private val tracer: Tracer
) {
    suspend fun scrapeAndIngest(targetUrl: String): ScrapeResult {
        val domain = runCatching { URI.create(targetUrl).host }.getOrDefault("unknown")

        return observationRegistry.observeSuspend(
            spanName = "scraper.ingest",
            tracer = tracer,
            lowCardinalityTags = mapOf("retailer.domain" to domain),
            highCardinalityTags = mapOf("target.url" to targetUrl)
        ) {
            val scraper = scraperRegistry.getScraperForUrl(targetUrl)
            val result = scraper.scrape(targetUrl)

            when (result) {
                is ScrapeResult.Success -> {
                    metricsRecorder.recordScrapeSuccess(domain, result.tier, result.latencyMs)
                    val event = RawScrapeEvent(
                        url = targetUrl,
                        retailerDomain = domain,
                        payload = result.payload,
                        latencyMs = result.latencyMs,
                    )
                    eventProducer.publishRawScrape(event)
                    log.info("Successfully scraped and dispatched event for url={} tier={}", targetUrl, result.tier)
                }
                is ScrapeResult.Failure -> {
                    val lastTier = result.attemptedTiers.lastOrNull() ?: ScrapeTier.FAST_PATH_JSON_LD
                    metricsRecorder.recordScrapeFailure(domain, lastTier, result.failureReason, 0L)
                    metricsRecorder.recordDlqPublished(domain, result.failureReason)
                    log.warn("Scrape failed for url {} (Reason: {}). Dispatching to DLQ...", targetUrl, result.failureReason)

                    val dlqEvent = ScrapeDeadLetterEvent.create(
                        url = targetUrl,
                        retailerDomain = domain,
                        failureReason = result.failureReason,
                        httpStatusCode = result.statusCode,
                        errorMessage = result.errorMessage,
                        attemptedTiers = result.attemptedTiers,
                        latencyMs = 0L,
                        rawHtml = result.rawSnippet
                    )
                    dlqProducer.publishDeadLetter(dlqEvent)
                }
                is ScrapeResult.RateLimited -> {
                    metricsRecorder.recordRateLimited(domain)
                    log.warn("Rate limited for domain {} on url {}. Retry after {}s", domain, targetUrl, result.retryAfterSeconds)
                }
            }
            result
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScrapeIngestionService::class.java)
    }
}
```

#### File: [`pulse-scraper/src/main/resources/application.yml`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/resources/application.yml)
```yaml
server:
  port: 8081

spring:
  application:
    name: pulse-scraper
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    template:
      observation-enabled: true

scraper:
  playwright:
    headless: true
    pool-size: 4
    timeout-ms: 25000
    slow-mo: 0.0

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: ${spring.application.name}
    distribution:
      percentiles-histogram:
        http.client.requests: true
        pulse.scraper.execution.duration: true
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318/v1/traces}
```

---

## Subtask 5.4: Pipeline Instrumentation, Kafka Trace Propagation & Metrics (`pulse-pipeline`)

### 1. What We Are Doing
We are upgrading `pulse-pipeline` to participate in end-to-end distributed traces across Kafka topics (`raw-scrapes` $\rightarrow$ `canonical-prices` & `raw-scrapes-dlq`) and recording ingestion telemetry:
1. **`KafkaConsumerConfig.kt` & `KafkaProducerConfig.kt`:** Enabling `factory.containerProperties.isObservationEnabled = true` on `ConcurrentKafkaListenerContainerFactory` and `template.setObservationEnabled(true)` on `KafkaTemplate`.
2. **`PipelineMetricsRecorder.kt`:** Tracks GTIN vs fallback resolution ratios, Redis GTIN cache hits/misses, TimescaleDB hypertable write durations, and DLQ audit writes.
3. **Instrumented `CanonicalResolver.kt`, `PricePersistenceService.kt`, and `RawScrapeKafkaConsumer.kt`:** Propagates the incoming Kafka consumer span into the coroutine via `observeSuspend` so R2DBC SQL queries and downstream `canonical-prices` Kafka messages inherit the parent `traceId` initiated by `pulse-scraper`.

### 2. Why We Are Doing It & Deep Mechanics

#### How W3C Trace Context Travels Over Apache Kafka Headers
When `template.setObservationEnabled(true)` is enabled on `KafkaTemplate`:
1. Before serializing a `RawScrapeEvent`, Spring Kafka's `KafkaTemplateObservationConvention` invokes the OpenTelemetry `TextMapPropagator`.
2. It injects a `traceparent` header (e.g. `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`) directly into the Kafka `ProducerRecord.headers()`.
3. When `pulse-pipeline` consumes the record with `containerProperties.isObservationEnabled = true`, Spring Kafka extracts the `traceparent` header and creates a `CONSUMER` child span linked to the scraper's `PRODUCER` span.
4. By capturing that active span before launching the coroutine (`observeSuspend`), the downstream `canonical-prices` producer injects the **same `traceId`** into the next Kafka topic, linking all 3 microservices in a single trace tree!

### 3. Proposed Code

#### File: `pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/metrics/PipelineMetricsRecorder.kt`
```kotlin
package com.pulseprice.pipeline.metrics

import com.pulseprice.common.model.ScrapeFailureReason
import com.pulseprice.common.observability.PulseMetricNames
import com.pulseprice.common.observability.PulseMetricNames.Tags
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class PipelineMetricsRecorder(private val meterRegistry: MeterRegistry) {

    fun recordCanonicalResolution(source: String) {
        // source: "REDIS_GTIN_HIT", "DB_GTIN_HIT", "NEW_GTIN_CREATED", "FALLBACK_BRAND_TITLE"
        Counter.builder(PulseMetricNames.PIPELINE_CANONICAL_RESOLUTION_TOTAL)
            .tag(Tags.RESOLUTION_SOURCE, source)
            .register(meterRegistry)
            .increment()
    }

    fun recordEventProcessed(retailer: String, outcome: String, durationMs: Long) {
        Counter.builder(PulseMetricNames.PIPELINE_EVENTS_PROCESSED_TOTAL)
            .tag(Tags.RETAILER, retailer.lowercase().removePrefix("www."))
            .tag(Tags.OUTCOME, outcome)
            .register(meterRegistry)
            .increment()

        Timer.builder(PulseMetricNames.PIPELINE_INGESTION_DURATION_SECONDS)
            .tag(Tags.RETAILER, retailer.lowercase().removePrefix("www."))
            .tag(Tags.OUTCOME, outcome)
            .publishPercentileHistogram(true)
            .register(meterRegistry)
            .record(Duration.ofMillis(durationMs))
    }

    fun recordDlqAudited(retailer: String, reason: ScrapeFailureReason) {
        Counter.builder(PulseMetricNames.PIPELINE_DLQ_AUDITED_TOTAL)
            .tag(Tags.RETAILER, retailer.lowercase().removePrefix("www."))
            .tag(Tags.FAILURE_REASON, reason.name)
            .register(meterRegistry)
            .increment()
    }
}
```

#### Updated File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/KafkaConsumerConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/config/KafkaConsumerConfig.kt)
Update `kafkaListenerContainerFactory`, `dlqKafkaListenerContainerFactory`, and `downstreamKafkaTemplate` to enable Micrometer Observation:
```kotlin
    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, RawScrapeEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, RawScrapeEvent>()
        factory.consumerFactory = consumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.containerProperties.isObservationEnabled = true // Phase 5: Extract W3C traceparent header
        factory.setConcurrency(3)
        return factory
    }

    @Bean
    fun dlqKafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, ScrapeDeadLetterEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, ScrapeDeadLetterEvent>()
        factory.consumerFactory = dlqConsumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.containerProperties.isObservationEnabled = true
        return factory
    }

    @Bean
    fun downstreamKafkaTemplate(): KafkaTemplate<String, Any> {
        return KafkaTemplate(downstreamProducerFactory()).apply {
            setObservationEnabled(true) // Phase 5: Inject W3C traceparent header into canonical-prices
        }
    }
```

#### Updated File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/scraper/kafka/KafkaProducerConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/scraper/kafka/KafkaProducerConfig.kt)
Enable observation on the primary `kafkaTemplate()`:
```kotlin
    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Any> = KafkaTemplate(producerFactory()).apply {
        setObservationEnabled(true)
    }
```

#### Updated File: [`pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/scraper/kafka/RawScrapeKafkaConsumer.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-pipeline/src/main/kotlin/com/pulseprice/pipeline/scraper/kafka/RawScrapeKafkaConsumer.kt)
```kotlin
package com.pulseprice.pipeline.scraper.kafka

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.common.observability.observeSuspend
import com.pulseprice.pipeline.metrics.PipelineMetricsRecorder
import com.pulseprice.pipeline.resolver.CanonicalResolver
import com.pulseprice.pipeline.service.PricePersistenceService
import io.micrometer.observation.ObservationRegistry
import io.micrometer.tracing.Tracer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import kotlin.system.measureTimeMillis

@Component
class RawScrapeKafkaConsumer(
    private val canonicalResolver: CanonicalResolver,
    private val pricePersistenceService: PricePersistenceService,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val metricsRecorder: PipelineMetricsRecorder,
    private val observationRegistry: ObservationRegistry,
    private val tracer: Tracer
) {
    companion object {
        private val log = LoggerFactory.getLogger(RawScrapeKafkaConsumer::class.java)
        const val TOPIC_DLT = "raw-scrapes-dlt"
    }

    private val scope = CoroutineScope(Dispatchers.IO)

    @KafkaListener(topics = ["raw-scrapes"], containerFactory = "kafkaListenerContainerFactory")
    fun onRawScrape(record: ConsumerRecord<String, RawScrapeEvent?>, acknowledgment: Acknowledgment) {
        val event = record.value()
        if (event == null) {
            log.warn("Received null/unparsable payload from partition={}, offset={}. Routing to DLT.", record.partition(), record.offset())
            kafkaTemplate.send(TOPIC_DLT, record.key() ?: "unknown", "CORRUPTED_EVENT")
            acknowledgment.acknowledge()
            return
        }

        // Capture current MDC (populated by Spring Kafka Observation listener on consumer thread)
        val parentMdc = MDCContext()

        scope.launch(parentMdc) {
            var outcome = "SUCCESS"
            val elapsed = measureTimeMillis {
                try {
                    observationRegistry.observeSuspend(
                        spanName = "pipeline.ingest.raw-scrape",
                        tracer = tracer,
                        lowCardinalityTags = mapOf("retailer" to event.retailerDomain),
                        highCardinalityTags = mapOf("event.id" to event.eventId, "product.url" to event.url)
                    ) {
                        val resolution = canonicalResolver.resolve(event)
                        pricePersistenceService.recordPrice(resolution, event)
                        acknowledgment.acknowledge()
                    }
                } catch (ex: Exception) {
                    outcome = "FAILURE"
                    log.error("Failed to process event [id={}]: {}. Routing to DLT.", event.eventId, ex.message, ex)
                    kafkaTemplate.send(TOPIC_DLT, record.key() ?: "unknown", event)
                    acknowledgment.acknowledge()
                }
            }
            metricsRecorder.recordEventProcessed(event.retailerDomain, outcome, elapsed)
        }
    }
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
    url: r2dbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5444}/${POSTGRES_DB:pulseprice}
    username: ${POSTGRES_USER:pulse}
    password: ${POSTGRES_PASSWORD:pulsepass}
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    listener:
      observation-enabled: true
    template:
      observation-enabled: true
    consumer:
      group-id: pulse-pipeline-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: "com.pulseprice.*"

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: ${spring.application.name}
    distribution:
      percentiles-histogram:
        pulse.pipeline.ingestion.duration: true
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318/v1/traces}
```

---

## Subtask 5.5: Metasearch API & Alert Engine Telemetry (`pulse-api`)

### 1. What We Are Doing
We are instrumenting `pulse-api` with:
1. **`ApiMetricsRecorder.kt`:** Records alert evaluations, alert triggers by `AlertType` (`TARGET_PRICE`, `PERCENTAGE_DROP`, `ALL_TIME_LOW`, `BACK_IN_STOCK`), cooldown suppressions, notification dispatch durations and delivery status per channel (`EMAIL`, `WEBHOOK`, `SSE`), and Redis L1 cache hit/miss ratios for `/search`, `/compare`, and `/history`.
2. **`PulseInfrastructureHealthIndicator.kt`:** Custom reactive health indicator verifying TimescaleDB hypertable readiness and Redis L1 cache responsiveness.
3. **`ApiObservabilityConfig.kt` & `KafkaApiConfig.kt`:** Enables Kafka listener and producer trace propagation on `canonical-prices` and `price-drop-alerts`, and adds a `WebFilter` that injects `X-Trace-Id` into every HTTP response header for instant client debugging.
4. **Instrumented `AlertEvaluationEngine.kt` & `NotificationDispatcher.kt`:** Records business metrics and trace spans across alert matching and multi-channel dispatch.

### 2. Why We Are Doing It & Deep Mechanics

#### `X-Trace-Id` Response Header & Cache Hit Ratio Optimization
- Returning `X-Trace-Id` on every `/api/v1/products/*` response allows frontend developers or API consumers experiencing slow responses or unexpected search rankings to paste the header directly into Grafana Tempo and inspect the exact SQL trigram query latency and Redis cache lookup state.
- Monitoring `pulse_api_cache_requests_total{cache_name="compare", cache_result="HIT|MISS"}` lets operators tune TTLs (`search-ttl-seconds`, `compare-ttl-seconds`) based on real user traffic patterns.

### 3. Proposed Code

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/metrics/ApiMetricsRecorder.kt`
```kotlin
package com.pulseprice.api.metrics

import com.pulseprice.common.model.AlertType
import com.pulseprice.common.model.NotificationChannel
import com.pulseprice.common.observability.PulseMetricNames
import com.pulseprice.common.observability.PulseMetricNames.Tags
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class ApiMetricsRecorder(private val meterRegistry: MeterRegistry) {

    fun recordAlertEvaluated(retailer: String, subscriptionCount: Int) {
        Counter.builder(PulseMetricNames.ALERT_EVALUATIONS_TOTAL)
            .tag(Tags.RETAILER, retailer.lowercase().removePrefix("www."))
            .register(meterRegistry)
            .increment(subscriptionCount.toDouble().coerceAtLeast(1.0))
    }

    fun recordAlertTriggered(alertType: AlertType, channel: NotificationChannel) {
        Counter.builder(PulseMetricNames.ALERT_TRIGGERS_TOTAL)
            .tag(Tags.ALERT_TYPE, alertType.name)
            .tag(Tags.CHANNEL, channel.name)
            .register(meterRegistry)
            .increment()
    }

    fun recordAlertSuppressedByCooldown(alertType: AlertType) {
        Counter.builder(PulseMetricNames.ALERT_COOLDOWN_SUPPRESSED_TOTAL)
            .tag(Tags.ALERT_TYPE, alertType.name)
            .register(meterRegistry)
            .increment()
    }

    fun recordNotificationDispatched(
        channel: NotificationChannel,
        delivered: Boolean,
        durationMs: Long
    ) {
        val outcome = if (delivered) "DELIVERED" else "FAILED"
        Counter.builder(PulseMetricNames.NOTIFICATION_DISPATCH_TOTAL)
            .tag(Tags.CHANNEL, channel.name)
            .tag(Tags.OUTCOME, outcome)
            .register(meterRegistry)
            .increment()

        Timer.builder(PulseMetricNames.NOTIFICATION_DURATION_SECONDS)
            .tag(Tags.CHANNEL, channel.name)
            .tag(Tags.OUTCOME, outcome)
            .publishPercentileHistogram(true)
            .register(meterRegistry)
            .record(Duration.ofMillis(durationMs))
    }

    fun recordCacheLookup(cacheName: String, hit: Boolean) {
        Counter.builder(PulseMetricNames.METASEARCH_CACHE_REQUESTS_TOTAL)
            .tag(Tags.CACHE_NAME, cacheName)
            .tag(Tags.CACHE_RESULT, if (hit) "HIT" else "MISS")
            .register(meterRegistry)
            .increment()
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/config/ApiObservabilityConfig.kt`
```kotlin
package com.pulseprice.api.config

import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.aop.ObservedAspect
import io.micrometer.tracing.Tracer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.server.WebFilter

@Configuration
class ApiObservabilityConfig {

    @Bean
    fun observedAspect(observationRegistry: ObservationRegistry): ObservedAspect {
        return ObservedAspect(observationRegistry)
    }

    /**
     * Attaches the active OpenTelemetry W3C Trace ID to every HTTP response
     * in the `X-Trace-Id` header for instant correlation in Grafana Tempo.
     */
    @Bean
    fun traceIdResponseHeaderFilter(tracer: Tracer): WebFilter {
        return WebFilter { exchange, chain ->
            exchange.response.beforeCommit {
                val traceId = tracer.currentSpan()?.context()?.traceId()
                if (!traceId.isNullOrBlank()) {
                    exchange.response.headers.set("X-Trace-Id", traceId)
                }
                reactor.core.publisher.Mono.empty()
            }
            chain.filter(exchange)
        }
    }
}
```

#### File: `pulse-api/src/main/kotlin/com/pulseprice/api/health/PulseInfrastructureHealthIndicator.kt`
```kotlin
package com.pulseprice.api.health

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.ReactiveHealthIndicator
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component("pulseInfrastructure")
class PulseInfrastructureHealthIndicator(
    private val databaseClient: DatabaseClient,
    private val redisTemplate: ReactiveStringRedisTemplate
) : ReactiveHealthIndicator {

    override fun health(): Mono<Health> {
        val dbCheck = databaseClient.sql("SELECT COUNT(*) AS cnt FROM timescaledb_information.hypertables")
            .map { row, _ -> row.get("cnt", java.lang.Long::class.java) ?: 0L }
            .first()
            .defaultIfEmpty(0L)

        val redisCheck = redisTemplate.connectionFactory.reactiveConnection.ping()
            .defaultIfEmpty("FAIL")

        return Mono.zip(dbCheck, redisCheck)
            .map { tuple ->
                val hypertableCount = tuple.t1
                val redisPing = tuple.t2
                if (redisPing.equals("PONG", ignoreCase = true)) {
                    Health.up()
                        .withDetail("timescaledb_hypertables", hypertableCount)
                        .withDetail("redis_ping", redisPing)
                        .build()
                } else {
                    Health.down()
                        .withDetail("timescaledb_hypertables", hypertableCount)
                        .withDetail("redis_ping", redisPing)
                        .build()
                }
            }
            .onErrorResume { ex ->
                Mono.just(Health.down(ex).build())
            }
    }
}
```

#### Updated File: [`pulse-api/src/main/kotlin/com/pulseprice/api/config/KafkaApiConfig.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/kotlin/com/pulseprice/api/config/KafkaApiConfig.kt)
Enable observation on `kafkaListenerContainerFactory`:
```kotlin
    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = consumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.containerProperties.isObservationEnabled = true // Phase 5: Distributed tracing across Kafka
        factory.setConcurrency(3)
        return factory
    }
```

#### File: [`pulse-api/src/main/resources/application.yml`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-api/src/main/resources/application.yml)
```yaml
server:
  port: 8000
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
    listener:
      observation-enabled: true
    template:
      observation-enabled: true
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

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: ${spring.application.name}
    distribution:
      percentiles-histogram:
        http.server.requests: true
        pulse.api.notifications.duration: true
  tracing:
    sampling:
      probability: 1.0
  otlp:
    tracing:
      endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318/v1/traces}
```

---

## Subtask 5.6: Structured Trace-Correlated Logging (`logback-spring.xml`)

### 1. What We Are Doing
We are providing a unified `logback-spring.xml` configuration for `pulse-scraper`, `pulse-pipeline`, and `pulse-api` that supports two profiles:
- **Default / Local Development:** Colorized console output enriched with `[service,traceId,spanId]` directly in every log line.
- **`json-logs` / Production Profile:** Structured JSON logs via `LogstashEncoder` including `traceId`, `spanId`, `service`, thread, logger, and stack trace hashes.

### 2. Why We Are Doing It & Deep Mechanics

#### Log-to-Trace Correlation
When an exception occurs in `NotificationDispatcher` or `PlaywrightHtmlFetcher`, operators searching logs need to know what triggered that execution upstream. Because Micrometer Tracing automatically populates SLF4J `MDC` keys `traceId` and `spanId` (and `CoroutineTraceContext` preserves them across suspension points), every single `log.info` / `log.error` line automatically embeds `[pulse-api,672f9a10b4c1e890a123456789abcdef,4123456789abcdef]`.

### 3. Proposed Code

#### File: `pulse-scraper/src/main/resources/logback-spring.xml` (Copy also to `pulse-pipeline/src/main/resources/logback-spring.xml` and `pulse-api/src/main/resources/logback-spring.xml`)
```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <include resource="org/springframework/boot/logging/logback/defaults.xml"/>

    <springProperty scope="context" name="APP_NAME" source="spring.application.name" defaultValue="pulse-service"/>

    <!-- Human-readable Console Appender with W3C traceId and spanId -->
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %5p [${APP_NAME},%X{traceId:-},%X{spanId:-}] [%15.15t] %-40.40logger{39} : %m%n${LOG_EXCEPTION_CONVERSION_WORD:-%wEx}</pattern>
            <charset>utf8</charset>
        </encoder>
    </appender>

    <!-- Structured JSON Appender for Production Log Aggregation -->
    <appender name="JSON_CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <customFields>{"service":"${APP_NAME}"}</customFields>
            <includeMdcKeyName>traceId</includeMdcKeyName>
            <includeMdcKeyName>spanId</includeMdcKeyName>
        </encoder>
    </appender>

    <springProfile name="!json-logs">
        <root level="INFO">
            <appender-ref ref="CONSOLE"/>
        </root>
    </springProfile>

    <springProfile name="json-logs">
        <root level="INFO">
            <appender-ref ref="JSON_CONSOLE"/>
        </root>
    </springProfile>

    <logger name="com.pulseprice" level="DEBUG"/>
</configuration>
```

---

## Subtask 5.7: Provisioned Grafana Operational Dashboard (`pulse-overview-dashboard.json`)

### 1. What We Are Doing
We are creating the complete Grafana dashboard definition (`observability/grafana/dashboards/pulse-overview-dashboard.json`) automatically loaded by Grafana on container startup. It visualizes:
1. **Scraper Throughput by Tier (`FAST_PATH_NEXT_DATA`, `FAST_PATH_JSON_LD`, `SLOW_PATH_PLAYWRIGHT`)**
2. **Scraper p95 / p99 Latency Histogram (with Tempo Exemplar links)**
3. **Anti-Bot Challenges & Rate-Limit Throttling Rate**
4. **Canonical Product Resolution Hit Ratio (Redis GTIN Hit vs DB Hit vs Fallback)**
5. **Pipeline Ingestion Throughput & Dead-Letter Queue (DLQ) Rate**
6. **Price Drop Alerts Triggered vs Cooldown Suppressed**
7. **Notification Dispatch Rate & Latency by Channel (`EMAIL`, `WEBHOOK`, `SSE`)**
8. **Metasearch API HTTP Request Rate, p99 Latency & L1 Redis Cache Hit Ratio**

### 2. Proposed Code

#### File: `observability/grafana/dashboards/pulse-overview-dashboard.json`
```json
{
  "annotations": { "list": [] },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "links": [],
  "panels": [
    {
      "title": "Scraper Throughput by Tier (req/s)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (tier, outcome) (rate(pulse_scraper_executions_total[1m]))",
          "legendFormat": "{{tier}} ({{outcome}})"
        }
      ]
    },
    {
      "title": "Scraper Execution Duration (p95 / p99)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 8, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum by (le, tier) (rate(pulse_scraper_execution_duration_seconds_bucket[5m])))",
          "legendFormat": "p95 {{tier}}",
          "exemplar": true
        },
        {
          "expr": "histogram_quantile(0.99, sum by (le, tier) (rate(pulse_scraper_execution_duration_seconds_bucket[5m])))",
          "legendFormat": "p99 {{tier}}",
          "exemplar": true
        }
      ]
    },
    {
      "title": "Anti-Bot Challenges & Rate-Limit Throttles",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 16, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (retailer) (rate(pulse_scraper_antibot_detected_total[5m]))",
          "legendFormat": "Anti-Bot: {{retailer}}"
        },
        {
          "expr": "sum by (retailer) (rate(pulse_scraper_ratelimited_total[5m]))",
          "legendFormat": "Rate-Limited: {{retailer}}"
        }
      ]
    },
    {
      "title": "Pipeline Ingestion Throughput & Latency (p95)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 0, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (retailer, outcome) (rate(pulse_pipeline_events_processed_total[1m]))",
          "legendFormat": "{{retailer}} ({{outcome}})"
        },
        {
          "expr": "histogram_quantile(0.95, sum by (le) (rate(pulse_pipeline_ingestion_duration_seconds_bucket[5m])))",
          "legendFormat": "p95 Ingestion Duration (s)",
          "exemplar": true
        }
      ]
    },
    {
      "title": "Canonical Product Resolution Strategy",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 8, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (resolution_source) (rate(pulse_pipeline_canonical_resolution_total[5m]))",
          "legendFormat": "{{resolution_source}}"
        }
      ]
    },
    {
      "title": "Dead-Letter Queue (DLQ) Failures by Reason",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 16, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (retailer, failure_reason) (rate(pulse_scraper_dlq_published_total[5m]))",
          "legendFormat": "{{retailer}} - {{failure_reason}}"
        }
      ]
    },
    {
      "title": "Price Drop Alerts Triggered vs Suppressed",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 0, "y": 16 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (alert_type, channel) (rate(pulse_api_alerts_triggered_total[5m]))",
          "legendFormat": "Triggered: {{alert_type}} ({{channel}})"
        },
        {
          "expr": "sum by (alert_type) (rate(pulse_api_alerts_cooldown_suppressed_total[5m]))",
          "legendFormat": "Suppressed: {{alert_type}}"
        }
      ]
    },
    {
      "title": "Notification Dispatch Rate & p95 Latency",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 8, "y": 16 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (channel, outcome) (rate(pulse_api_notifications_dispatched_total[5m]))",
          "legendFormat": "{{channel}} ({{outcome}})"
        },
        {
          "expr": "histogram_quantile(0.95, sum by (le, channel) (rate(pulse_api_notifications_duration_seconds_bucket[5m])))",
          "legendFormat": "p95 {{channel}} (s)"
        }
      ]
    },
    {
      "title": "Metasearch API Request Rate & p99 Latency",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 8, "x": 16, "y": 16 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "sum by (uri, status) (rate(http_server_requests_seconds_count{application=\"pulse-api\"}[1m]))",
          "legendFormat": "{{uri}} [{{status}}]"
        },
        {
          "expr": "histogram_quantile(0.99, sum by (le, uri) (rate(http_server_requests_seconds_bucket{application=\"pulse-api\"}[5m])))",
          "legendFormat": "p99 {{uri}}",
          "exemplar": true
        }
      ]
    }
  ],
  "refresh": "10s",
  "schemaVersion": 39,
  "title": "PulsePrice — Distributed Metasearch & Alert Engine Overview",
  "uid": "pulse-price-overview",
  "version": 1
}
```

---

## Subtask 5.8: End-to-End Verification & Smoke Testing Guide

### 1. Start the Observability & Data Infrastructure
```bash
podman compose up -d
# Or with Docker:
# docker compose up -d
```

### 2. Verify Health & Prometheus Metrics Endpoints
Once `pulse-scraper` (port `8081`), `pulse-pipeline` (port `8082`), and `pulse-api` (port `8000`) are running:
```bash
# 1. Check Reactive Health Indicators (Playwright, TimescaleDB Hypertables, Redis, Kafka)
curl -s http://localhost:8081/actuator/health | jq .
curl -s http://localhost:8082/actuator/health | jq .
curl -s http://localhost:8000/actuator/health | jq .

# 2. Inspect Prometheus Metric Output & Exemplars
curl -s http://localhost:8000/actuator/prometheus | grep -E "pulse_api|http_server_requests"

# 3. Verify X-Trace-Id Header on Metasearch API Responses
curl -i "http://localhost:8000/api/v1/products/search?q=sony&page=0&size=10"
```

### 3. Explore Distributed Traces & Dashboards in Grafana
1. Open **Grafana** at `http://localhost:3000` (auto-logged in as `Admin`, or `admin` / `admin`).
2. Navigate to **Dashboards -> PulsePrice -> PulsePrice — Distributed Metasearch & Alert Engine Overview** to view real-time scraper tier distributions, pipeline GTIN cache hit rates, and alert triggers.
3. Navigate to **Explore -> Tempo** and search by the `X-Trace-Id` header returned from your API call or select a trace from `pulse-scraper` to inspect the full waterfall span across `pulse-scraper` $\rightarrow$ Kafka $\rightarrow$ `pulse-pipeline` $\rightarrow$ TimescaleDB $\rightarrow$ `pulse-api`.

# Phase 1: Ingestion Engine & Sandbox Verification

This document provides a subtask breakdown of **Phase 1**, explaining the architectural motivations, internal mechanics, and proposed code implementations.

---

## Architecture & Directory Layout

```text
pulse-price/
├── settings.gradle.kts
├── build.gradle.kts
├── pulse-common/
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/pulseprice/common/
│       └── model/
│           ├── ProductRawPayload.kt
│           └── ScrapeResult.kt
└── pulse-scraper/
    ├── build.gradle.kts
    └── src/
        ├── main/
        │   ├── kotlin/com/pulseprice/scraper/
        │   │   ├── ScraperApplication.kt
        │   │   ├── RetailerScraper.kt
        │   │   ├── DefaultRetailerScraper.kt
        │   │   ├── extractor/JsonLdExtractor.kt
        │   │   └── ratelimit/RedisTokenBucketRateLimiter.kt
        │   └── resources/application.yml
        └── test/
            ├── kotlin/com/pulseprice/scraper/RetailerScraperIntegrationTest.kt
            └── resources/application-test.yml
```

---

## Subtask 1.1: Multi-Module Gradle Architecture & Build Setup

### 1. What We Are Doing
We are setting up a Gradle multi-module project using Kotlin DSL (`.kts`). The modules are:
- `pulse-common`: Pure Kotlin library containing shared domain models and contracts.
- `pulse-scraper`: Spring Boot service with non-blocking WebFlux, Kotlin Coroutines, Jsoup, and Reactive Redis.

### 2. Why We Are Doing It & Deep Mechanics
- **Separation of Concerns:** Separating domain contracts (`pulse-common`) from ingestion mechanics (`pulse-scraper`) ensures downstream modules in future phases (e.g., `pulse-storage`, `pulse-api`) can consume product models without pulling in heavy scraping dependencies like Jsoup or Playwright.
- **Java 21 + Kotlin 2.0:** Enables modern language features, pattern matching, records/data classes, and high-performance Coroutine primitives.
- **Spring Boot 3.3.x with Reactive Stack:** WebFlux + Project Reactor + Kotlin Coroutines (`kotlinx-coroutines-reactor`) allow asynchronous, non-blocking I/O without the memory footprint of thread-per-request blocking servers.

### 3. Proposed Code

#### File: [`settings.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/settings.gradle.kts)
```kotlin
rootProject.name = "pulse-price"

include("pulse-common")
include("pulse-scraper")
```

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

    dependencies {
        val implementation by configurations
        val testImplementation by configurations

        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlin:kotlin-stdlib")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.8.1")

        testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
        testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions {
            jvmTarget = "21"
            freeCompilerArgs = listOf("-Xjsr305=strict")
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
```

#### File: [`pulse-common/build.gradle.kts`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/build.gradle.kts)
```kotlin
plugins {
    kotlin("jvm")
}

dependencies {
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")
}
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

    // HTML parsing & extraction
    implementation("org.jsoup:jsoup:1.18.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")

    // Testing: WireMock & Testcontainers
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.wiremock:wiremock-standalone:3.9.1")
    testImplementation("com.redis:testcontainers-redis:2.2.2")
    testImplementation("org.testcontainers:junit-jupiter:1.20.1")
}
```

---

## Subtask 1.2: Shared Domain Models & Envelope Design

### 1. What We Are Doing
We are creating the foundational data structures:
- [`ProductRawPayload`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/model/ProductRawPayload.kt): Encapsulates extracted product data (title, prices, currency, identifiers like GTIN/SKU, in-stock status, and raw JSON-LD payload).
- [`ScrapeResult`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/model/ScrapeResult.kt): Sealed hierarchy representing all possible scrape outcomes (`Success`, `RateLimited`, `Failure`).

### 2. Why We Are Doing It & Deep Mechanics
- **Financial Precision:** We use `BigDecimal` for `currentPrice` and `originalPrice`. IEEE-754 floating-point primitives (`Double`, `Float`) introduce binary precision errors (e.g. `0.1 + 0.2 = 0.30000000000000004`), which can corrupt historical price triggers and price-drop calculations.
- **Exhaustive Pattern Matching:** Kotlin's `sealed interface` guarantees compile-time completeness in `when` expressions. Downstream consumers (scrapers, Kafka producers) must explicitly handle `Success`, `RateLimited`, and `Failure` without falling back to ambiguous default branches.
- **Auditability via `rawJsonLd`:** Retaining the unparsed JSON-LD string allows debugging schema changes and re-running parsers during DOM drift without re-scraping the target site.

### 3. Proposed Code

#### File: [`pulse-common/src/main/kotlin/com/pulseprice/common/model/ProductRawPayload.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/model/ProductRawPayload.kt)
```kotlin
package com.pulseprice.common.model

import java.math.BigDecimal
import java.time.Instant

data class ProductRawPayload(
    val title: String,
    val currentPrice: BigDecimal,
    val currency: String,
    val originalPrice: BigDecimal? = null,
    val sku: String? = null,
    val gtin: String? = null,
    val brand: String? = null,
    val inStock: Boolean = true,
    val rawJsonLd: String? = null,
    val scrapedAt: Instant = Instant.now()
)
```

#### File: [`pulse-common/src/main/kotlin/com/pulseprice/common/model/c.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-common/src/main/kotlin/com/pulseprice/common/model/ScrapeResult.kt)
```kotlin
package com.pulseprice.common.model

sealed interface ScrapeResult {
    data class Success(
        val url: String,
        val payload: ProductRawPayload,
        val latencyMs: Long
    ) : ScrapeResult

    data class RateLimited(
        val url: String,
        val domain: String,
        val retryAfterSeconds: Long
    ) : ScrapeResult

    data class Failure(
        val url: String,
        val statusCode: Int?,
        val errorMessage: String,
        val isRetryable: Boolean
    ) : ScrapeResult
}
```

---

## Subtask 1.3: Distributed Token Bucket Rate Limiting with Redis & Lua

### 1. What We Are Doing
We are implementing [`RedisTokenBucketRateLimiter`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/ratelimit/RedisTokenBucketRateLimiter.kt) which limits request velocity on a per-domain basis (e.g. `walmart.com`, `target.com`).

### 2. Why We Are Doing It & Deep Mechanics
- **Why Token Bucket vs. Fixed Window:** Fixed-window rate limiters allow double the burst capacity across window boundaries (e.g. 5 requests at second 0.9 and 5 requests at second 1.1). Token bucket guarantees a smooth average rate (`refillRatePerSec`) while allowing controlled bursts up to `capacity`.
- **Atomic Redis Lua Scripting:** In a distributed multi-instance crawler, checking available tokens and decrementing them must happen atomically. A Redis Lua script runs inside a single Redis thread, preventing race conditions (Time-of-Check to Time-of-Use) without the latency of distributed Redis locks (Redlock).
- **Reactive Integration with Lettuce:** Uses Spring's `ReactiveStringRedisTemplate` converted to Coroutines with `.awaitSingle()`. The calling thread is never blocked while waiting for Redis network I/O.

### 3. Proposed Code

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/ratelimit/RedisTokenBucketRateLimiter.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/ratelimit/RedisTokenBucketRateLimiter.kt)
```kotlin
package com.pulseprice.scraper.ratelimit

import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class RedisTokenBucketRateLimiter(
    private val redisTemplate: ReactiveStringRedisTemplate
) {
    /**
     * Lua script implementing Token Bucket:
     * - KEYS[1]: Redis hash key (ratelimit:{domain})
     * - ARGV[1]: Bucket capacity
     * - ARGV[2]: Refill rate in tokens per second
     * - ARGV[3]: Tokens requested
     * - ARGV[4]: Current timestamp (epoch seconds)
     * Returns: 1 if acquired, 0 if rate limited.
     */
    private val luaScript = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local requested = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])

        local bucket = redis.call('HMGET', key, 'tokens', 'last_updated')
        local tokens = tonumber(bucket[1])
        local last_updated = tonumber(bucket[2])

        if tokens == nil then
            tokens = capacity
            last_updated = now
        else
            local delta = math.max(0, now - last_updated)
            tokens = math.min(capacity, tokens + (delta * refill_rate))
            last_updated = now
        end

        if tokens >= requested then
            tokens = tokens - requested
            redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
            redis.call('EXPIRE', key, 3600)
            return 1
        else
            redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
            return 0
        end
    """.trimIndent()

    private val redisScript = DefaultRedisScript(luaScript, Long::class.java)

    suspend fun tryAcquire(
        domain: String,
        capacity: Long = 5,
        refillRatePerSec: Long = 2,
        requested: Long = 1
    ): Boolean {
        val key = "ratelimit:$domain"
        val nowSec = Instant.now().epochSecond

        val result = redisTemplate.execute(
            redisScript,
            listOf(key),
            listOf(capacity.toString(), refillRatePerSec.toString(), requested.toString(), nowSec.toString())
        ).awaitSingle()

        return result == 1L
    }
}
```

---

## Subtask 1.4: SEO Structured Data Extraction with Jsoup & Jackson

### 1. What We Are Doing
We are building [`JsonLdExtractor`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/extractor/JsonLdExtractor.kt), which extracts product information directly from `<script type="application/ld+json">` elements using Schema.org ontology.

### 2. Why We Are Doing It & Deep Mechanics
- **Resilience to DOM Drift:** Scraping product titles and prices via CSS selectors (e.g. `div.price-box > span.current-price`) is fragile. Modern web apps frequently change class names, use CSS modules, or perform A/B layout experiments.
- **Standardized Retail Schema:** Retailers embed Schema.org `Product` JSON-LD so search engines (Google, Bing) can index products and display rich snippets. This structured data contains verified identifiers (GTIN-13, UPC, SKU), prices, currency codes, and stock status.
- **Graph Traversal:** Retailers frequently wrap products inside `@graph` arrays alongside breadcrumbs and seller organizations. The extractor recursively searches for `@type: "Product"` nodes.

### 3. Proposed Code

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/extractor/JsonLdExtractor.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/extractor/JsonLdExtractor.kt)
```kotlin
package com.pulseprice.scraper.extractor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.model.ProductRawPayload
import org.jsoup.nodes.Document
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class JsonLdExtractor(private val objectMapper: ObjectMapper) {

    fun extract(doc: Document): ProductRawPayload? {
        val scripts = doc.select("script[type=application/ld+json]")
        for (script in scripts) {
            val jsonContent = script.data().trim()
            if (jsonContent.isBlank()) continue

            runCatching {
                val node = objectMapper.readTree(jsonContent)
                val productNode = findProductNode(node)
                if (productNode != null) {
                    return parseProduct(productNode, jsonContent)
                }
            }
        }
        return null
    }

    private fun findProductNode(node: JsonNode): JsonNode? {
        if (node.isArray) {
            return node.firstOrNull { it.path("@type").asText().equals("Product", ignoreCase = true) }
        }
        if (node.path("@type").asText().equals("Product", ignoreCase = true)) {
            return node
        }
        if (node.has("@graph")) {
            return findProductNode(node.path("@graph"))
        }
        return null
    }

    private fun parseProduct(node: JsonNode, rawJson: String): ProductRawPayload {
        val title = node.path("name").asText("")
        val brand = node.path("brand").path("name").asText(null) ?: node.path("brand").asText(null)
        val sku = node.path("sku").asText(null)
        val gtin = node.path("gtin13").asText(null)
            ?: node.path("gtin").asText(null)
            ?: node.path("isbn").asText(null)

        val offers = node.path("offers")
        val offerNode = if (offers.isArray) offers.firstOrNull() ?: offers else offers

        val priceStr = offerNode.path("price").asText("0.00").replace(",", "")
        val price = runCatching { BigDecimal(priceStr) }.getOrDefault(BigDecimal.ZERO)
        val currency = offerNode.path("priceCurrency").asText("USD")
        val availability = offerNode.path("availability").asText("")
        val inStock = availability.contains("InStock", ignoreCase = true)

        return ProductRawPayload(
            title = title,
            currentPrice = price,
            currency = currency,
            sku = sku,
            gtin = gtin,
            brand = brand,
            inStock = inStock,
            rawJsonLd = rawJson
        )
    }
}
```

---

## Subtask 1.5: Non-Blocking Scraper Strategy & Coroutine Pipeline

### 1. What We Are Doing
We are defining the scraper contract [`RetailerScraper`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/RetailerScraper.kt) and implementing [`DefaultRetailerScraper`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/DefaultRetailerScraper.kt), which integrates rate-limiting, non-blocking HTTP fetch, and parsing.

### 2. Why We Are Doing It & Deep Mechanics
- **Suspending Functions vs. Thread Blocking:** Traditional `RestTemplate` or Jsoup `Jsoup.connect(url).get()` blocks an OS worker thread while waiting for remote retail web servers to respond (often 300ms–2000ms). Under high concurrency, this exhausts thread pools. WebClient + Coroutines suspend execution and free the thread back to the event loop.
- **Dispatching CPU-Intensive Tasks:** HTML string parsing with Jsoup is CPU-intensive. We explicitly wrap `Jsoup.parse` inside `withContext(Dispatchers.IO)` so it does not block the Netty event loop threads that process inbound network frames.
- **Graceful Error Classification:** Scrape outcomes categorize HTTP 429 and 5xx errors as `isRetryable = true`, allowing future stages to apply exponential backoff.

### 3. Proposed Code

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/RetailerScraper.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/RetailerScraper.kt)
```kotlin
package com.pulseprice.scraper

import com.pulseprice.common.model.ScrapeResult

interface RetailerScraper {
    fun supports(domain: String): Boolean
    suspend fun scrape(targetUrl: String): ScrapeResult
}
```

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/DefaultRetailerScraper.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/DefaultRetailerScraper.kt)
```kotlin
package com.pulseprice.scraper

import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.scraper.extractor.JsonLdExtractor
import com.pulseprice.scraper.ratelimit.RedisTokenBucketRateLimiter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jsoup.Jsoup
import org.springframework.http.HttpStatusCode
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitExchange
import org.springframework.web.reactive.function.client.awaitBody
import java.net.URI
import kotlin.system.measureTimeMillis

@Service
class DefaultRetailerScraper(
    private val webClient: WebClient,
    private val rateLimiter: RedisTokenBucketRateLimiter,
    private val jsonLdExtractor: JsonLdExtractor
) : RetailerScraper {

    override fun supports(domain: String): Boolean = true

    override suspend fun scrape(targetUrl: String): ScrapeResult {
        val uri = URI.create(targetUrl)
        val domain = uri.host ?: "unknown"

        val permitted = rateLimiter.tryAcquire(domain)
        if (!permitted) {
            return ScrapeResult.RateLimited(url = targetUrl, domain = domain, retryAfterSeconds = 1)
        }

        var html = ""
        var statusCode = 200
        val latency = measureTimeMillis {
            try {
                html = webClient.get()
                    .uri(targetUrl)
                    .header("User-Agent", "PulsePriceBot/1.0 (+https://pulseprice.internal)")
                    .awaitExchange { clientResponse ->
                        statusCode = clientResponse.statusCode().value()
                        if (clientResponse.statusCode().is4xxClientError || clientResponse.statusCode().is5xxServerError) {
                            throw UpstreamScrapeException(clientResponse.statusCode(), "HTTP $statusCode from $domain")
                        }
                        clientResponse.awaitBody<String>()
                    }
            } catch (ex: UpstreamScrapeException) {
                return ScrapeResult.Failure(
                    url = targetUrl,
                    statusCode = ex.status.value(),
                    errorMessage = ex.message ?: "Upstream error",
                    isRetryable = ex.status.value() in listOf(429, 500, 502, 503, 504)
                )
            } catch (ex: Exception) {
                return ScrapeResult.Failure(
                    url = targetUrl,
                    statusCode = null,
                    errorMessage = ex.localizedMessage ?: "Unknown network failure",
                    isRetryable = true
                )
            }
        }

        val doc = withContext(Dispatchers.IO) {
            Jsoup.parse(html, targetUrl)
        }

        val payload = jsonLdExtractor.extract(doc)
            ?: return ScrapeResult.Failure(
                url = targetUrl,
                statusCode = statusCode,
                errorMessage = "No valid Schema.org/Product JSON-LD found",
                isRetryable = false
            )

        return ScrapeResult.Success(
            url = targetUrl,
            payload = payload,
            latencyMs = latency
        )
    }

    private class UpstreamScrapeException(val status: HttpStatusCode, message: String) : RuntimeException(message)
}
```

#### File: [`pulse-scraper/src/main/kotlin/com/pulseprice/scraper/ScraperApplication.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/main/kotlin/com/pulseprice/scraper/ScraperApplication.kt)
```kotlin
package com.pulseprice.scraper

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.web.reactive.function.client.WebClient

@SpringBootApplication
class ScraperApplication {

    @Bean
    fun webClient(): WebClient = WebClient.builder().build()
}

fun main(args: Array<String>) {
    runApplication<ScraperApplication>(*args)
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
```

---

## Subtask 1.6: Automated Sandbox Verification Tests with WireMock & Testcontainers

### 1. What We Are Doing
We are writing integration tests in [`RetailerScraperIntegrationTest`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/test/kotlin/com/pulseprice/scraper/RetailerScraperIntegrationTest.kt) running against:
- A local **WireMock** server simulating realistic retail endpoints.
- A **Redis Testcontainer** validating real atomic Lua script token bucket behavior.

### 2. Why We Are Doing It & Deep Mechanics
- **Hermetic Testing:** Integration tests that hit live retail websites are brittle, trigger IP bans, fail behind corporate firewalls, and cause test unpredictability.
- **Simulating Chaos & Edge Cases:** WireMock allows us to simulate exact HTTP 429 throttles, 503 gateway outages, malformed HTML, and rapid burst traffic to verify our retry logic and rate limiters.
- **Real Redis Container:** Embedded in-memory mock Redis libraries often fail to execute custom Lua scripts properly. Testcontainers spins up a lightweight Docker Redis 7 instance, guaranteeing identical behavior to production.

### 3. Proposed Code

#### File: [`pulse-scraper/src/test/kotlin/com/pulseprice/scraper/RetailerScraperIntegrationTest.kt`](file:///home/kyammanuru/github.com/rajanikanthy-1979/pulse-price/pulse-scraper/src/test/kotlin/com/pulseprice/scraper/RetailerScraperIntegrationTest.kt)
```kotlin
package com.pulseprice.scraper

import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo
import com.github.tomakehurst.wiremock.junit5.WireMockTest
import com.pulseprice.common.model.ScrapeResult
import com.redis.testcontainers.RedisContainer
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.math.BigDecimal

@SpringBootTest
@Testcontainers
@WireMockTest
class RetailerScraperIntegrationTest @Autowired constructor(
    private val scraper: RetailerScraper
) {
    companion object {
        @Container
        private val redis = RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME.withTag("7.2-alpine"))

        @JvmStatic
        @DynamicPropertySource
        fun registerRedisProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.data.redis.host") { redis.host }
            registry.add("spring.data.redis.port") { redis.firstMappedPort }
        }
    }

    @Test
    fun `should parse valid product json-ld successfully`(wm: WireMockRuntimeInfo) = runBlocking {
        val targetPath = "/products/sony-wh1000xm5"
        val sampleHtml = """
            <!DOCTYPE html>
            <html>
            <head>
                <script type="application/ld+json">
                {
                  "@context": "https://schema.org/",
                  "@type": "Product",
                  "name": "Sony WH-1000XM5 Wireless Headphones",
                  "brand": { "@type": "Brand", "name": "Sony" },
                  "sku": "SONY-WH1000XM5-BLK",
                  "gtin13": "0027242923591",
                  "offers": {
                    "@type": "Offer",
                    "priceCurrency": "USD",
                    "price": "348.00",
                    "availability": "https://schema.org/InStock"
                  }
                }
                </script>
            </head>
            <body>Mock store page</body>
            </html>
        """.trimIndent()

        stubFor(get(urlEqualTo(targetPath))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "text/html")
                .withBody(sampleHtml)))

        val result = scraper.scrape("${wm.httpBaseUrl}$targetPath")

        assertTrue(result is ScrapeResult.Success)
        val success = result as ScrapeResult.Success
        assertEquals("Sony WH-1000XM5 Wireless Headphones", success.payload.title)
        assertEquals(BigDecimal("348.00"), success.payload.currentPrice)
        assertEquals("USD", success.payload.currency)
        assertEquals("0027242923591", success.payload.gtin)
        assertTrue(success.payload.inStock)
    }

    @Test
    fun `should handle 503 upstream as retryable failure`(wm: WireMockRuntimeInfo) = runBlocking {
        stubFor(get(urlEqualTo("/error-503"))
            .willReturn(aResponse().withStatus(503).withBody("Service Unavailable")))

        val result = scraper.scrape("${wm.httpBaseUrl}/error-503")

        assertTrue(result is ScrapeResult.Failure)
        val failure = result as ScrapeResult.Failure
        assertEquals(503, failure.statusCode)
        assertTrue(failure.isRetryable)
    }

    @Test
    fun `should rate limit excessive requests`(wm: WireMockRuntimeInfo) = runBlocking {
        stubFor(get(urlMatching("/rate-test/.*"))
            .willReturn(aResponse().withStatus(200).withBody("<html></html>")))

        val results = (1..10).map { id ->
            scraper.scrape("${wm.httpBaseUrl}/rate-test/$id")
        }

        assertTrue(results.any { it is ScrapeResult.RateLimited }, "At least one request should have hit rate limiter")
    }
}
```

---

## Subtask 1.7: Manual Execution & Verification

Once you have reviewed and placed the files above into their respective locations, execute these commands in your shell:

1. **Initialize the Gradle Wrapper** (if not already initialized):
   ```bash
   gradle wrapper --gradle-version 8.8
   ```

2. **Compile the multi-module project**:
   ```bash
   ./gradlew compileKotlin
   ```

3. **Run the integration test suite**:
   ```bash
   ./gradlew test
   ```

package com.pulseprice.scraper.service

import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.scraper.extractor.JsonLdExtractor
import com.pulseprice.scraper.ratelimit.RedisTokenBucketRateLimiter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jsoup.Jsoup
import org.springframework.http.HttpStatusCode
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import java.net.URI
import kotlin.system.measureTimeMillis

class DefaultRetailerScraper(
    private val webClient: WebClient,
    private val rateLimiter: RedisTokenBucketRateLimiter,
    private val jsonLdExtractor: JsonLdExtractor
) : RetailerScraper {
    override fun supports(domain: String): Boolean = true

    override suspend fun scrape(targetUrl: String): ScrapeResult {
        val uri = URI(targetUrl)
        val domain = uri.host ?: "unknown"

        val permitted = rateLimiter.tryAcquire(domain)
        if (!permitted) {
            return ScrapeResult.RateLimited(url = targetUrl, domain = domain, retryAfterSeconds = 1L)
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
                    isRetryable = ex.status.value() in listOf(429, 500, 502, 504)
                )
            } catch (ex: Exception) {
                return ScrapeResult.Failure(
                    url = targetUrl,
                    statusCode = null,
                    errorMessage = ex.localizedMessage ?: "Unknown network failure",
                    isRetryable = false
                )
            }
        }

        val doc = withContext(Dispatchers.IO) {
            Jsoup.parse(html, targetUrl)
        }

        val payload = jsonLdExtractor.extract(doc) ?: return ScrapeResult.Failure(
            url = targetUrl,
            statusCode = statusCode,
            errorMessage = "No valid schema.org/Product JSON-LD found",
            isRetryable = false
        )
        return ScrapeResult.Success(
            url = targetUrl,
            payload = payload,
            latencyMs = latency
        )
    }

    private class UpstreamScrapeException(val status: HttpStatusCode, message: String): RuntimeException(message)
}

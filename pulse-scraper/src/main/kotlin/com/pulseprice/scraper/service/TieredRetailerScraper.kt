package com.pulseprice.scraper.service

import com.pulseprice.common.model.ScrapeFailureReason
import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.common.model.ScrapeTier
import com.pulseprice.scraper.browser.PlaywrightHtmlFetcher
import com.pulseprice.scraper.detection.AntiBotChallengeDetector
import com.pulseprice.scraper.detector.DomDriftDetector
import com.pulseprice.scraper.extractor.FastPathCompositeExtractor
import com.pulseprice.scraper.ratelimit.RedisTokenBucketRateLimiter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jsoup.Jsoup
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import java.net.URI
import kotlin.system.measureTimeMillis

import org.springframework.web.reactive.function.client.awaitExchange
import java.math.BigDecimal

open class TieredRetailerScraper(
    private val webClient: WebClient,
    private val rateLimiter: RedisTokenBucketRateLimiter,
    private val fastPathCompositeExtractor: FastPathCompositeExtractor,
    private val playwrightFetcher: PlaywrightHtmlFetcher,
    private val antiBotDetector: AntiBotChallengeDetector,
    private val domDriftDetector: DomDriftDetector
): RetailerScraper {
    override fun supports(domain: String): Boolean = true

    override suspend fun scrape(targetUrl: String): ScrapeResult {
        val uri = URI(targetUrl)
        val domain = uri.host ?: "unknown"

        val permitted = rateLimiter.tryAcquire(domain)
        if (!permitted) {
            return ScrapeResult.RateLimited(
                url = targetUrl,
                domain = domain,
                retryAfterSeconds = 1L
            )
        }
        val attemptedTiers = mutableListOf<ScrapeTier>()
        var fastPathHtml = ""
        var fastPathStatus = 200

        val fastPathLatency = measureTimeMillis {
            try {
                fastPathHtml = webClient.get()
                    .uri(targetUrl)
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .awaitExchange { clientResponse ->
                        fastPathStatus = clientResponse.statusCode().value()
                        clientResponse.awaitBody<String>()
                    }
            } catch (ex: Exception) {
                log.warn("Fast-path network request failed for {}: {}", targetUrl, ex.message)
                fastPathStatus = 500
            }

        }

        val isFastPathBlocked = antiBotDetector.isChallenge(fastPathStatus, fastPathHtml)
        if (!isFastPathBlocked && fastPathHtml.isNotBlank()) {
            val doc = withContext(Dispatchers.IO) {
                Jsoup.parse(fastPathHtml, targetUrl)
            }
            val payload = fastPathCompositeExtractor.extract(doc)
            if (payload != null && payload.currentPrice > BigDecimal.ZERO) {
                attemptedTiers.add(payload.extractionTier)
                return ScrapeResult.Success(
                    url = targetUrl,
                    payload = payload,
                    latencyMs = fastPathLatency,
                    tier = payload.extractionTier
                )
            }
        }
        attemptedTiers.add(ScrapeTier.FAST_PATH_JSON_LD)
        log.info("Fast-path extraction insufficient for {}. Escalating to Tier 2 Playwright fallback...", targetUrl)

        var slowPathStatus = 200
        var slowPathHtml = ""

        val slowPathLatency = measureTimeMillis {
            try {
                val (status, html) = playwrightFetcher.fetchRenderedHtml(targetUrl)
                slowPathStatus = status
                slowPathHtml = html
            } catch (ex: Exception) {
                log.error("Playwright headless fallback failed for {}: {}", targetUrl, ex.message)
                return ScrapeResult.Failure(
                    url = targetUrl,
                    statusCode = 500,
                    errorMessage = "Playwright rendering failed: ${ex.message}",
                    attemptedTiers = attemptedTiers + ScrapeTier.SLOW_PATH_PLAYWRIGHT,
                    rawSnippet = fastPathHtml.take(4096)
                )
            }
        }
        attemptedTiers.add(ScrapeTier.SLOW_PATH_PLAYWRIGHT)

        val renderedDoc = withContext(Dispatchers.IO) {
            Jsoup.parse(slowPathHtml, targetUrl)
        }

        val renderedPayload = fastPathCompositeExtractor.extract(renderedDoc)

        if (renderedPayload != null && renderedPayload.currentPrice > BigDecimal.ZERO) {
            val finalPayload = renderedPayload.copy(extractionTier = ScrapeTier.SLOW_PATH_PLAYWRIGHT)
            return ScrapeResult.Success(
                url = targetUrl,
                payload = finalPayload,
                latencyMs = slowPathLatency,
                tier = ScrapeTier.SLOW_PATH_PLAYWRIGHT
            )
        }

        val failureReason = domDriftDetector.analyze(
            statusCode = slowPathStatus,
            doc = renderedDoc,
            rawHtml = slowPathHtml,
            extractedPayload = renderedPayload,
        )

        return ScrapeResult.Failure(
            url = targetUrl,
            statusCode = slowPathStatus,
            errorMessage = "Failed extraction across all tiers: $failureReason",
            failureReason = failureReason,
            isRetryable = failureReason == ScrapeFailureReason.ANTI_BOT_CHALLENGE || failureReason == ScrapeFailureReason.RATE_LIMITED,
            attemptedTiers = attemptedTiers,
            rawSnippet = slowPathHtml.ifBlank { fastPathHtml }.take(8192)
        )
    }


    companion object {
        private val log: Logger = LoggerFactory.getLogger(TieredRetailerScraper::class.java)
    }
}

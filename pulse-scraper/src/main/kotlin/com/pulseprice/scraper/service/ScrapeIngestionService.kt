package com.pulseprice.scraper.service

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.common.event.ScrapeDeadLetterEvent
import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.pipeline.scraper.kafka.ScrapeEventProducer
import com.pulseprice.scraper.kafka.ScrapeDlqProducer
import com.pulseprice.scraper.retailers.RetailerScraperRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI

@Service
class ScrapeIngestionService(
    private val scraperRegistry: RetailerScraperRegistry,
    private val eventProducer: ScrapeEventProducer,
    private val dlqProducer: ScrapeDlqProducer
) {
    suspend fun scrapeAndIngest(targetUrl: String): ScrapeResult {
        val domain = runCatching { URI.create(targetUrl).host }.getOrDefault("unknown")
        val scraper = scraperRegistry.getScraperForUrl(targetUrl)
        val result = scraper.scrape(targetUrl)

        when (result) {
            is ScrapeResult.Success -> {
                val event = RawScrapeEvent(
                    url = targetUrl,
                    retailerDomain = domain,
                    payload = result.payload,
                    latencyMs = result.latencyMs,
                )
                eventProducer.publishRawScrape(event)
                log.info("Successfully scraped and dispatched event for url $targetUrl")
            }
            is ScrapeResult.Failure -> {
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
                log.warn("Rate limited for domain {} on url {}. Retry after {}s", domain, targetUrl, result.retryAfterSeconds)
            }
        }
        return result
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScrapeIngestionService::class.java)
    }
}

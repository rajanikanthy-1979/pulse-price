package com.pulseprice.scraper.service

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.common.model.ScrapeResult
import com.pulseprice.pipeline.scraper.kafka.ScrapeEventProducer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI

@Service
class ScrapeIngestionService(
    private val scraper: RetailerScraper,
    private val eventProducer: ScrapeEventProducer
) {
    suspend fun scrapeAndIngest(targetUrl: String): ScrapeResult {
        val result = scraper.scrape(targetUrl)
        if (result is ScrapeResult.Success) {
            val domain = runCatching { URI.create(targetUrl).host }.getOrDefault("unknown")
            val event = RawScrapeEvent(
                url = targetUrl,
                retailerDomain = domain,
                payload = result.payload,
                latencyMs = result.latencyMs,
            )
            eventProducer.publishRawScrape(event)
            log.info("Successfully scraped and dispatched event for url $targetUrl")
        } else {
            log.warn("Scrape unsuccessful for url $targetUrl (Result: {})", result)
        }
        return result
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScrapeIngestionService::class.java)
    }
}

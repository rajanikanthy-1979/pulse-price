package com.pulseprice.scraper.retailers

import com.pulseprice.scraper.service.RetailerScraper
import org.springframework.stereotype.Component
import java.net.URI

@Component
class RetailerScraperRegistry(
    private val scrapers: List<RetailerScraper>,
    private val genericScraper: GenericRetailerScraper
) {
    fun getScraperForUrl(url: String): RetailerScraper {
        val domain = runCatching {
            URI.create(url).host ?: ""
        }.getOrDefault("")
        return scrapers.firstOrNull { it !is GenericRetailerScraper && it.supports(domain) }
            ?: genericScraper
    }
}

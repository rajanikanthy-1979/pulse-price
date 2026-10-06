package com.pulseprice.scraper.service

import com.pulseprice.common.model.ScrapeResult

interface RetailerScraper {
    fun supports(domain: String): Boolean
    suspend fun scrape(targetUrl: String): ScrapeResult
}

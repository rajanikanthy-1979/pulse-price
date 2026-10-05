package com.pulseprice.scraper.services

import com.pulseprice.common.model.ScrapeResult

interface RetailerScraper {
    fun supports(domain: String): Boolean
    suspend fun scrape(targetUrl: String): ScrapeResult
}
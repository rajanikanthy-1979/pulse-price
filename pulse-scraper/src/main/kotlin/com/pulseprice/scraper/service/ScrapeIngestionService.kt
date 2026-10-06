package com.pulseprice.scraper.service

import com.pulseprice.scraper.kafka.ScrapeEventProducer
import org.springframework.stereotype.Service

@Service
class ScrapeIngestionService(
    private val scraper: RetailerScraper,
    private val eventProducer: ScrapeEventProducer
) {

}

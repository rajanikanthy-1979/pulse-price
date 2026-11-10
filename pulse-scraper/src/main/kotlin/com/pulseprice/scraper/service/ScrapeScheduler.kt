package com.pulseprice.scraper.service

import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@EnableScheduling
class ScrapeScheduler(private val scrapeIngestionService: ScrapeIngestionService) {
    @Scheduled(fixedRate = 60_000 * 5)
    fun scrape() {
        val targetUrls = arrayOf(
            // --- Group 1: Grocery & Beverages (Safeway / Target / Walmart) ---
            // 1. Safeway — Coca-Cola Classic 12-Pack (12 fl oz cans)
            "https://www.safeway.com/shop/pd/coca-cola-soda-pop-classic-12-12-fl-oz/108010222",

            // 2. Target — Coca-Cola Classic 12-Pack (Cross-retailer GTIN match with #1)
            "https://www.target.com/p/coca-cola-12pk-12-fl-oz-cans/-/A-12953464",

            // 3. Safeway — Oreo Chocolate Sandwich Cookies Family Size (18.12 oz)
            "https://www.safeway.com/shop/pd/oreo-chocolate-sandwich-cookies-family-size-18-12-oz/960109033",

            // 4. Target — Oreo Chocolate Sandwich Cookies Family Size (Cross-retailer GTIN match with #3)
            "https://www.target.com/p/oreo-chocolate-sandwich-cookies-family-size-18-12oz/-/A-50087770",

            // 5. Safeway — Red Bull Energy Drink 4-Pack (8.4 fl oz cans)
            "https://www.safeway.com/shop/pd/red-bull-energy-drink-4-8-4-fl-oz/108050131",

            // 6. Safeway — Doritos Nacho Cheese Tortilla Chips Party Size (14.5 oz)
            "https://www.safeway.com/shop/pd/doritos-tortilla-chips-nacho-cheese-flavored-party-size-14-5-oz/960495479",

            // --- Group 2: Electronics & Tech (Target / Best Buy / Walmart) ---
            // 7. Target — Apple AirPods Pro 2 (USB-C)
            "https://www.target.com/p/apple-airpods-pro-2/-/A-85978612",

            // 8. Best Buy — Apple AirPods Pro 2 (Cross-retailer GTIN match with #7)
            "https://www.bestbuy.com/site/apple-airpods-pro-2-wireless-active-noise-cancelling-earbuds-with-hearing-aid-feature-white/6447382.p?skuId=6447382",

            // 9. Best Buy — Sony WH-1000XM5 Wireless Noise-Canceling Headphones (Black)
            "https://www.bestbuy.com/site/sony-wh1000xm5-wireless-noise-canceling-over-the-ear-headphones-black/6505727.p?skuId=6505727",

            // 10. Walmart — Nintendo Switch OLED Model (White Joy-Con)
            "https://www.walmart.com/ip/Nintendo-Switch-OLED-Model-w-White-Joy-Con/910582148"
        )

        runBlocking {
            targetUrls.asFlow().collect {
                val result = scrapeIngestionService.scrapeAndIngest(it)
                log.info("Result: $result")
            }
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(ScrapeScheduler::class.java)
    }
}

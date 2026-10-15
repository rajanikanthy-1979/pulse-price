package com.pulseprice.scraper.retailers

import com.pulseprice.scraper.browser.PlaywrightHtmlFetcher
import com.pulseprice.scraper.detection.AntiBotChallengeDetector
import com.pulseprice.scraper.detector.DomDriftDetector
import com.pulseprice.scraper.extractor.FastPathCompositeExtractor
import com.pulseprice.scraper.ratelimit.RedisTokenBucketRateLimiter
import com.pulseprice.scraper.service.TieredRetailerScraper
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient

@Component
class TargetRetailerScraper(
    webClient: WebClient,
    rateLimiter: RedisTokenBucketRateLimiter,
    fastPathCompositeExtractor: FastPathCompositeExtractor,
    playwrightHtmlFetcher: PlaywrightHtmlFetcher,
    antiBotChallengeDetector: AntiBotChallengeDetector,
    domDriftDetector: DomDriftDetector
) : TieredRetailerScraper(webClient,
        rateLimiter,
        fastPathCompositeExtractor,
        playwrightHtmlFetcher,
    antiBotChallengeDetector,
    domDriftDetector) {

    override fun supports(domain: String): Boolean {
        return domain.contains("target.com", ignoreCase = true)
    }
}

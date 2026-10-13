package com.pulseprice.scraper.detector

import com.pulseprice.common.model.ProductRawPayload
import com.pulseprice.common.model.ScrapeFailureReason
import com.pulseprice.scraper.detection.AntiBotChallengeDetector
import org.jsoup.nodes.Document
import org.springframework.stereotype.Component

@Component
class DomDriftDetector(private val antiBotChallengeDetector: AntiBotChallengeDetector) {

    fun analyze(
        statusCode: Int?,
        doc: Document?,
        rawHtml: String,
        extractedPayload: ProductRawPayload?
    ): ScrapeFailureReason {
        // Check for anti-bot blocks first
        if (antiBotChallengeDetector.isChallenge(statusCode, rawHtml)) {
            return ScrapeFailureReason.ANTI_BOT_CHALLENGE
        }

        if (statusCode != null && statusCode >= 400) {
            return ScrapeFailureReason.HTTP_STATUS_ERROR
        }

        if (rawHtml.isBlank() || doc == null || doc.body().text().isBlank()) {
            return ScrapeFailureReason.EMPTY_PAYLOAD
        }

        // Check for CAPTCHA markers specifically
        val text = doc.text().lowercase()
        if (text.contains("captcha") || text.contains("verify you are human")) {
            return ScrapeFailureReason.CAPTCHA_DETECTED
        }

        // If HTTP 200 and substantial body exists (>1000 chars), but payload is missing or zero price -> DOM drift
        if (extractedPayload == null || extractedPayload.currentPrice <= java.math.BigDecimal.ZERO) {
            return ScrapeFailureReason.DOM_DRIFT
        }

        // Confidence evaluation
        if (extractedPayload.confidenceScore < 0.6) {
            return ScrapeFailureReason.DOM_DRIFT
        }

        return ScrapeFailureReason.DOM_DRIFT
    }
}

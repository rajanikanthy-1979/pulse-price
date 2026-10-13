package com.pulseprice.scraper.detection

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class AntiBotChallengeDetector {

    fun isChallenge(statusCode: Int?, html: String): Boolean {
        if (statusCode == 403 || statusCode == 429 || statusCode == 503) {
            return true
        }
        if (html.isBlank()) return false
        val lower = html.lowercase()


        // 1. Cloudflare Turnstile & Challenge
        if (lower.contains("cf-browser-verification") ||
            lower.contains("just a moment...") ||
            lower.contains("cloudflare turnstile") ||
            lower.contains("challenges.cloudflare.com") ||
            (lower.contains("ray id:") && lower.contains("enable cookies"))
        ) {
            return true
        }

        // 2. PerimeterX / HUMAN
        if (lower.contains("px-captcha") ||
            lower.contains("client.perimeterx.net") ||
            lower.contains("access to this page has been denied") ||
            lower.contains("human verification")
        ) {
            return true
        }

        // 3. Akamai Bot Manager
        if (lower.contains("ak-challenge") ||
            lower.contains("akamai-bm") ||
            lower.contains("bm-telemetry")
        ) {
            return true
        }

        // 4. Amazon Robot Check
        if (lower.contains("robot check") && lower.contains("validatecaptcha")) {
            return true
        }

        // 5. DataDome
        if (lower.contains("datadome.js") || lower.contains("geo.captcha-delivery.com")) {
            return true
        }

        return false
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(AntiBotChallengeDetector::class.java)
    }
}

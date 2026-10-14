package com.pulseprice.scraper.browser

import com.microsoft.playwright.Browser
import com.microsoft.playwright.BrowserContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets

@Component
class PlaywrightStealthCustomizer {
    private val stealthScript: String by  lazy {
        val resource = ClassPathResource("stealth.js")
        resource.inputStream.bufferedReader(StandardCharsets.UTF_8).use { it.readText() }
    }

    fun createStealthContext(browser: Browser): BrowserContext {
        val context = browser.newContext(
            Browser.NewContextOptions()
                .setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
                .setViewportSize(1920, 1080)
                .setDeviceScaleFactor(1.0)
                .setIsMobile(false)
                .setHasTouch(false)
                .setLocale("en-US")
                .setTimezoneId("America/New_York")
                .setExtraHTTPHeaders(
                    mapOf(
                        "sec-ch-ua" to "\"Chromium\";v=\"128\", \"Not;A=Brand\";v=\"24\", \"Google Chrome\";v=\"128\"",
                        "sec-ch-ua-mobile" to "?0",
                        "sec-ch-ua-platform" to "\"Windows\"",
                        "Accept-Language" to "en-US,en;q=0.9",
                        "Upgrade-Insecure-Requests" to "1"
                    )
                )
        )
        context.addInitScript(stealthScript)
        return context
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PlaywrightStealthCustomizer::class.java)
    }
}

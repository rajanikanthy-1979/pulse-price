package com.pulseprice.scraper.browser

import com.microsoft.playwright.Browser
import com.microsoft.playwright.BrowserType
import com.microsoft.playwright.Playwright
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class PlaywrightBrowserManager(
    @Value("\${scraper.playwright.headless:true}") private val headless: Boolean,
    @Value("\${scraper.playwright.slow-mo:0}") private val slowMo: Double
) {

    private var playwright: Playwright? = null
    private var browser: Browser? = null

    @PostConstruct
    fun initialize() {
        log.info("Initializing Microsoft Playwright Chromium instance (headless={})...", headless)
        playwright = Playwright.create()
        browser = playwright!!.chromium().launch(
            BrowserType.LaunchOptions()
                .setHeadless(headless)
                .setSlowMo(slowMo)
                .setArgs(
                    listOf(
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-infobars",
                        "--window-position=0,0",
                        "--ignore-certifcate-errors",
                        "--ignore-certifcate-errors-spki-list",
                        "--disable-accelerated-2d-canvas",
                        "--disable-gpu"
                    )
                )
        )
        log.info("Playwright Chromium browser initialized successfully.")
    }

    fun getBrowser(): Browser {
        return browser ?: throw IllegalStateException("Playwright Browser is not initialized.")
    }

    @PreDestroy
    fun close() {
        log.info("Shutting down Playwright Chromium browser and driver...")
        runCatching { browser?.close() }
        runCatching { playwright?.close() }
        log.info("Playwright shutdown complete.")
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(PlaywrightBrowserManager::class.java)
    }
}

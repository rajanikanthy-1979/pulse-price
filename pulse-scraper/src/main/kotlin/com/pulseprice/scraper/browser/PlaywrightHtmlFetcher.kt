package com.pulseprice.scraper.browser

import com.microsoft.playwright.Page
import com.microsoft.playwright.options.LoadState
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import kotlin.time.Duration.Companion.milliseconds

@Component
class PlaywrightHtmlFetcher(
    private val browserManager: PlaywrightBrowserManager,
    private val stealthCustomizer: PlaywrightStealthCustomizer,
    @Value("\${scraper.playwright.pool-size:4}") private val poolSize: Int,
    @Value("\${scraper.playwright.timeout-ms:20000}") private val timeoutMs: Long
) {

    private val threadPool = Executors.newFixedThreadPool(poolSize)
    private val playwrightDispatcher = threadPool.asCoroutineDispatcher()
    private val concurrencyLimiter = Semaphore(poolSize)



    suspend fun fetchRenderedHtml(url: String): Pair<Int, String> {
        concurrencyLimiter.acquire()
        return try {
            withContext(playwrightDispatcher) {
                withTimeout(timeoutMs.milliseconds) {
                    val browser = browserManager.getBrowser()
                    val context = stealthCustomizer.createStealthContext(browser)
                    try {
                        val page = context.newPage()
                        page.route("**/*.{png,jpg,jpeg,webp,gif,svg,woff,woff2,ttf,eot,mp4,webm,css}") { route ->
                            route.abort()
                        }
                        page.route("**/*analytics*") { route -> route.abort() }
                        page.route("**/*doubleclick*") { route -> route.abort() }

                        val response = page.navigate(
                            url,
                            Page.NavigateOptions().setTimeout(timeoutMs.toDouble())
                        )

                        // Wait for DOMContentLoaded and hydration
                        page.waitForLoadState(LoadState.DOMCONTENTLOADED)
                        runCatching {
                            page.waitForLoadState(LoadState.NETWORKIDLE, Page.WaitForLoadStateOptions().setTimeout(3000.0))
                        }

                        val status = response?.status() ?: 200
                        val content = page.content()
                        Pair(status, content)
                    } finally {
                        runCatching { context.close() }
                    }

                }
            }
        } finally {
            concurrencyLimiter.release()
        }
    }


    companion object {
        private val log: Logger = LoggerFactory.getLogger(PlaywrightHtmlFetcher::class.java)
    }
}

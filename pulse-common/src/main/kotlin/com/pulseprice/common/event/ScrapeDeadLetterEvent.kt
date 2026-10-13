package com.pulseprice.common.event

import com.pulseprice.common.model.ScrapeFailureReason
import com.pulseprice.common.model.ScrapeTier
import java.time.Instant
import java.util.UUID

data class ScrapeDeadLetterEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val url: String,
    val retailerDomain: String,
    val failureReason: ScrapeFailureReason,
    val httpStatusCode: Int?,
    val errorMessage: String,
    val attemptedTiers: List<ScrapeTier>,
    val latencyMs: Long,
    val rawHtmlSnippet: String? = null,
    val failedAt: Instant = Instant.now(),
    val schemaVersion: Int = 1
) {
    companion object {
        private const val MAX_SNIPPET_LENGTH = 32768

        fun create(
            url: String,
            retailerDomain: String,
            failureReason: ScrapeFailureReason,
            httpStatusCode: Int?,
            errorMessage: String,
            attemptedTiers: List<ScrapeTier>,
            latencyMs: Long,
            rawHtml: String?
        ): ScrapeDeadLetterEvent {
            val snippet = rawHtml?.take(MAX_SNIPPET_LENGTH)
            return ScrapeDeadLetterEvent(
                url = url,
                retailerDomain = retailerDomain,
                failureReason = failureReason,
                httpStatusCode = httpStatusCode,
                errorMessage = errorMessage,
                attemptedTiers = attemptedTiers,
                latencyMs = latencyMs,
                rawHtmlSnippet = snippet
            )
        }
    }
}

package com.pulseprice.common.model

sealed interface ScrapeResult {
    data class Success(
        val url: String,
        val payload: ProductRawPayload,
        val latencyMs: Long,
        val tier: ScrapeTier
    ) : ScrapeResult

    data class RateLimited(
        val url: String,
        val domain: String,
        val retryAfterSeconds: Long,
    ) : ScrapeResult

    data class Failure(
        val url: String,
        val statusCode: Int?,
        val errorMessage: String,
        val failureReason: ScrapeFailureReason = ScrapeFailureReason.HTTP_STATUS_ERROR,
        val isRetryable: Boolean = false,
        val attemptedTiers: List<ScrapeTier> = emptyList(),
        val rawSnippet: String? = null
    ) : ScrapeResult
}

package com.pulseprice.common.model

sealed interface ScrapeResult {
    data class Success (
        val url: String,
        val payload: ProductRawPayload,
        val latencyMs: Long
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
        val isRetryable: Boolean = false,
    ) : ScrapeResult
}
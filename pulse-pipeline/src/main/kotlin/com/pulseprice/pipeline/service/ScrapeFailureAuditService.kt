package com.pulseprice.pipeline.service

import com.pulseprice.common.event.ScrapeDeadLetterEvent
import kotlinx.coroutines.reactive.awaitSingle
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class ScrapeFailureAuditService(private val databaseClient: DatabaseClient) {
    suspend fun recordFailure(event: ScrapeDeadLetterEvent) {
        val query = """
            INSERT INTO scrape_failures (
                id, failed_at, url, retailer_domain, failure_reason,
                http_status_code, error_message, attempted_tiers, html_snippet
            ) VALUES (
                :id, :failedAt, :url, :retailerDomain, :failureReason,
                :httpStatusCode, :errorMessage, :attemptedTiers, :htmlSnippet
            )
        """.trimIndent()

        runCatching {
            databaseClient.sql(query)
                .bind("id", UUID.fromString(event.eventId))
                .bind("failedAt", event.failedAt)
                .bind("url", event.url)
                .bind("retailerDomain", event.retailerDomain)
                .bind("failureReason", event.failureReason.name)
                .bind("httpStatusCode", event.httpStatusCode ?: 0)
                .bind("errorMessage", event.errorMessage)
                .bind("attemptedTiers", event.attemptedTiers.joinToString(",") { it.name })
                .bind("htmlSnippet", event.rawHtmlSnippet ?: "")
                .fetch()
                .rowsUpdated()
                .awaitSingle()

            log.info("Persisted scrape failure audit record in TimescaleDB for event {}", event.eventId)
        }.onFailure { ex ->
            log.error("Failed to persist scrape failure audit record: {}", ex.message, ex)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScrapeFailureAuditService::class.java)
    }
}

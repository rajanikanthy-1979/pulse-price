package com.pulseprice.pipeline.domain

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant
import java.util.UUID

@Table("scrape_failures")
data class ScrapeFailureEntity(
    @Id
    val id: UUID? = null,
    @Column("failed_at")
    val failedAt: Instant = Instant.now(),
    @Column("url")
    val url: String,
    @Column("retailer_domain")
    val retailerDomain: String,
    @Column("failure_reason")
    val failureReason: String,
    @Column("http_status_code")
    val httpStatusCode: Int?,
    @Column("error_message")
    val errorMessage: String,
    @Column("attempted_tiers")
    val attemptedTiers: String,
    @Column("html_snippet")
    val htmlSnippet: String?
)

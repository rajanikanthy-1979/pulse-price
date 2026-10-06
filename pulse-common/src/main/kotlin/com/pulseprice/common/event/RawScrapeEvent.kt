package com.pulseprice.common.event

import com.pulseprice.common.model.ProductRawPayload
import java.time.Instant
import java.util.UUID

data class RawScrapeEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val url: String,
    val retailerDomain: String,
    val payload: ProductRawPayload,
    val latencyMs: Long,
    val scrapedAt: Instant = Instant.now(),
    val schemaVersion: Int = 1
)

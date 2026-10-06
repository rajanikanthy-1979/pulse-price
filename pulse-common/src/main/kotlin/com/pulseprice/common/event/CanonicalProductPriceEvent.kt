package com.pulseprice.common.event

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class CanonicalProductPriceEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val canonicalProductId: UUID,
    val retailerProductId: UUID,
    val retailer: String,
    val gtin: String?,
    val price: BigDecimal,
    val originalPrice: BigDecimal? = null,
    val currency: String,
    val inStock: Boolean,
    val recordedAt: Instant = Instant.now()
)

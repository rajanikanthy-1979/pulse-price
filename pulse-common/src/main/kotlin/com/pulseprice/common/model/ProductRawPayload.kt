package com.pulseprice.common.model

import java.math.BigDecimal
import java.time.Instant

data class ProductRawPayload(
 val title: String,
 val currentPrice: BigDecimal,
 val originalPrice: BigDecimal? = null,
 val sku: String? = null,
 val gtin: String? = null,
 val brand: String? = null,
 val inStock: Boolean = true,
 val rawJsonLd: String? = null,
 val scrapedAt: Instant = Instant.now(),
 val currency: String
)
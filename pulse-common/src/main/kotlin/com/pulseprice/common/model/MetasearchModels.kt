package com.pulseprice.common.model

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class ProductSearchHit (
    val id: UUID,
    val gtin: String?,
    val brand: String?,
    val model: String?,
    val title: String,
    val lowestPrice: BigDecimal?,
    val highestPrice: BigDecimal?,
    val currency: String,
    val retailerCount: Int,
    val inStockCount: Int,
    val similarityScore: Double
)

data class ProductSearchResult (
    val items: List<ProductSearchHit>,
    val page: Int,
    val size: Int,
    val totalElements: Long,
    val totalPages: Int
)

data class RetailOfferDto (
    val retailerProductId: UUID,
    val retailer: String,
    val retailerSku: String?,
    val url: String,
    val title: String,
    val currentPrice: BigDecimal,
    val originalPrice: BigDecimal?,
    val currency: String,
    val inStock: Boolean,
    val priceDifferenceFromLowest: BigDecimal,
    val lastUpdated: Instant,
    val isBestPrice: Boolean,
)

data class ProductComparisonResponse (
    val canonicalProductId: UUID,
    val gtin: String?,
    val brand: String?,
    val model: String?,
    val title: String,
    val lowestPrice: BigDecimal?,
    val highestPrice: BigDecimal?,
    val priceSpread: BigDecimal,
    val bestRetailer: String,
    val currency: String,
    val offers: List<RetailOfferDto>
)

data class PriceHistoryPointDto (
    val bucketTimestamp: Instant,
    val minPrice: BigDecimal,
    val maxPrice: BigDecimal,
    val avgPrice: BigDecimal,
    val sampleCount: Long
)

data class PriceHistoryResponse (
    val canonicalProductId: UUID,
    val timeRange: String,
    val currentLowestPrice: BigDecimal?,
    val allTimeLow: BigDecimal?,
    val allTimeHigh: BigDecimal?,
    val averagePrice: BigDecimal?,
    val points: List<PriceHistoryPointDto>
)

data class PriceStreamUpdate (
    val canonicalProductId: UUID,
    val retailerProductId: UUID,
    val retailer: String,
    val price: BigDecimal,
    val originalPrice: BigDecimal?,
    val currency: String,
    val inStock: Boolean,
    val recordedAt: Instant,
)

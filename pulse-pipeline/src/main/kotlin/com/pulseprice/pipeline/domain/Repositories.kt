package com.pulseprice.pipeline.domain

import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface CanonicalProductRepository : CoroutineCrudRepository<CanonicalProductEntity, UUID> {
    suspend fun findByGtin(gtin: String): CanonicalProductEntity?

    @Query("SELECT * FROM canonical_products WHERE LOWER(brand) = LOWER(:brand) AND LOWER(title) = LOWER(:title) LIMIT 1")
    suspend fun findByBrandAndTitle(brand: String, title: String): CanonicalProductEntity?
}

@Repository
interface RetailerProductRepository: CoroutineCrudRepository<RetailerProductEntity, UUID> {
    suspend fun findByUrl(url: String): RetailerProductEntity?
    suspend fun findByRetailerAndRetailerSku(retailer: String, retailerSku: String) : RetailerProductEntity
}

@Repository
interface PriceHistoryRepository : CoroutineCrudRepository<PriceHistoryEntity, UUID> {
    @Query("""
        INSERT INTO price_history (recorded_at, retailer_product_id, canonical_product_id, price, original_price, currency, in_stock)
        VALUES (:recordedAt, :retailerProductId, :canonicalProductId, :price, :originalPrice, :currency, :inStock)
    """)
    suspend fun insertPricePoint(
        recordedAt: java.time.Instant,
        retailerProductId: UUID,
        canonicalProductId: UUID,
        price: java.math.BigDecimal,
        originalPrice: java.math.BigDecimal?,
        currency: String,
        inStock: Boolean
    ): Int
}



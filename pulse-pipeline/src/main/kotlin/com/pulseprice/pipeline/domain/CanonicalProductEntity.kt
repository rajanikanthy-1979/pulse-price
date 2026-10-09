package com.pulseprice.pipeline.domain

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Table("canonical_products")
data class CanonicalProductEntity(
    @Id
    val id: UUID? = null,
    val gtin: String?,
    val brand: String?,
    val model: String? = null,
    val title: String,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now(),
)

@Table("retailer_products")
data class RetailerProductEntity(
    @Id
    val id: UUID? = null,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    val retailer: String,
    @Column("retailer_sku") val retailerSku: String?,
    val url: String,
    val title: String,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now(),
)

@Table("price_history")
data class PriceHistoryEntity(
    @Column("recorded_at") val recordedAt: Instant = Instant.now(),
    @Column("retailer_product_id") val retailerProductId: UUID,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    val price: BigDecimal,
    @Column("original_price") val originalPrice: BigDecimal? = null,
    val currency: String = "USD",
    @Column("in_stock") val inSTock: Boolean = true

)

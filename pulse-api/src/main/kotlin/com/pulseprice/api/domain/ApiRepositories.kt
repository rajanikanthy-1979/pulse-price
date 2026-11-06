package com.pulseprice.api.domain

import com.pulseprice.common.model.PriceHistoryPointDto
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.repository.Modifying
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOneOrNull
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID
import javax.naming.directory.SearchResult

@Repository
interface CanonicalProductRepository : CoroutineCrudRepository<CanonicalProductEntity, UUID> {
    suspend fun findByGtin(gtin: String): CanonicalProductEntity?
}

@Repository
interface RetailProductRepository: CoroutineCrudRepository<RetailerProductEntity, UUID> {
    suspend fun findByCanonicalProductId(canonicalProductId: String): RetailerProductEntity?
}

@Repository
interface PriceAlertSubscriptionRepository: CoroutineCrudRepository<PriceAlertSubscriptionEntity, UUID> {
    fun findByCanonicalProductId(canonicalProductId: UUID): Flow<PriceAlertSubscriptionEntity>
    fun findByUserEmail(userEmail: String): Flow<PriceAlertSubscriptionEntity>

    @Modifying
    @Query(
        """
            UPDATE price_alert_subscriptions
            SET last_triggered_at = :lastTriggeredAt, updated_at = NOW()
            WHERE id = :id
        """
    )
    suspend fun updateLastTriggered(id: UUID, lastTriggeredAt: Instant): Int
}

@Repository
interface PriceAlertHistoryRepository: CoroutineCrudRepository<PriceAlertHistoryEntity, UUID> {
    @Query("""
        INSERT INTO price_alert_history (
            id, subscription_id, canonical_product_id, retailer_product_id,
            triggered_at, trigger_type, old_price, new_price,
            price_drop_amount, price_drop_percentage, channel, destination, status, error_message
        ) VALUES (
            :id, :subscriptionId, :canonicalProductId, :retailerProductId,
            :triggeredAt, :triggerType, :oldPrice, :newPrice,
            :priceDropAmount, :priceDropPercentage, :channel, :destination, :status, :errorMessage
        )
    """)
    suspend fun insertAuditRecord(
        id: UUID,
        subscriptionId: UUID,
        canonicalProductId: UUID,
        retailerProductId: UUID,
        triggeredAt: Instant,
        triggerType: String,
        oldPrice: BigDecimal?,
        newPrice: BigDecimal,
        priceDropAmount: BigDecimal?,
        priceDropPercentage: BigDecimal?,
        channel: String,
        destination: String,
        status: String,
        errorMessage: String?
    ): Int
}

/**
 * Dedicated Custom Repository executing optimized PostgreSQL Trigram Metasearch
 * and TimescaleDB analytical time-bucket aggregations
 */
@Repository
class ProductSearchCustomRepository(
   private val client: DatabaseClient,
    private val template: R2dbcEntityTemplate
){
    data class SearchRow(
        val id: UUID,
        val gtin: String?,
        val brand: String?,
        val model: String?,
        val title: String,
        val lowestPrice: BigDecimal?,
        val highestPrice: BigDecimal?,
        val currency: String?,
        val retailerCount: Long,
        val inStockCount: Long,
        val similarityScore: Double
    )

    fun searchProducts(
        query: String,
        brand: String?,
        limit: Int,
        offset: Int
    ): Flow<SearchRow> {
        val sql = """
            SELECT
             cp.id,
             cp.gtin,
             cp.brand,
             cp.model,
             cp.title,
             MIN(ph.price) AS lowest_price,
             MAX(ph.price) AS highest_price,
             COALESCE(MAX(ph.currency), 'USD') AS currency,
             COUNT(DISTINCT rp.id) AS retailer_count,
                COUNT(DISTINCT CASE WHEN ph.in_stock THEN rp.id END) AS in_stock_count,
                GREATEST(
                    similarity(cp.title, :query),
                    similarity(COALESCE(cp.brand, ''), :query)
                ) AS sim_score
            FROM canonical_products cp
            LEFT JOIN retailer_products rp ON rp.canonical_product_id = cp.id
            LEFT JOIN LATERAL (
                SELECT price, currency, in_stock
                FROM price_history
                WHERE retailer_product_id = rp.id
                ORDER BY recorded_at DESC
                LIMIT 1
            ) ph ON TRUE
            WHERE (:query = '' OR cp.title % :query OR cp.title ILIKE CONCAT('%', :query, '%') OR cp.brand ILIKE CONCAT('%', :query, '%'))
              AND (:brand IS NULL OR LOWER(cp.brand) = LOWER(:brand))
            GROUP BY cp.id, cp.gtin, cp.brand, cp.model, cp.title
            ORDER BY sim_score DESC, lowest_price ASC NULLS LAST
            LIMIT :limit OFFSET :offset
        """.trimIndent()

        var spec = client.sql(sql)
            .bind("query", query)
            .bind("limit", limit)
            .bind("offset", offset)

        spec = if (brand != null) spec.bind("brand", brand) else spec.bindNull("brand", String::class.java)

        return spec.map { row, _ ->
            SearchRow(
                id = row.get("id", UUID::class.java)!!,
                gtin = row.get("gtin", String::class.java),
                brand = row.get("brand", String::class.java),
                model = row.get("model", String::class.java),
                title = row.get("title", String::class.java)!!,
                lowestPrice = row.get("lowest_price", BigDecimal::class.java),
                highestPrice = row.get("highest_price", BigDecimal::class.java),
                currency = row.get("currency", String::class.java) ?: "USD",
                retailerCount = row.get("retailer_count", java.lang.Long::class.java)?.toLong() ?: 0L,
                inStockCount = row.get("in_stock_count", java.lang.Long::class.java)?.toLong() ?: 0L,
                similarityScore = row.get("sim_score", java.lang.Double::class.java)?.toDouble() ?: 0.0
            )
        }.all().asFlow()
    }

    suspend fun countSearchResults(query: String, brand: String?): Long {
        val sql = """
             SELECT COUNT(*) AS total
            FROM canonical_products cp
            WHERE (:query = '' OR cp.title % :query OR cp.title ILIKE CONCAT('%', :query, '%') OR cp.brand ILIKE CONCAT('%', :query, '%'))
              AND (:brand IS NULL OR LOWER(cp.brand) = LOWER(:brand))
        """.trimIndent()

        var spec = client.sql(sql)
            .bind("query", query)

        spec = if (brand != null) spec.bind("brand", brand) else spec.bindNull("brand", String::class.java)

        val result = spec.map { row, _ ->
            row.get("total", java.lang.Long::class.java)?.toLong() ?: 0L }
            .awaitOneOrNull()

        return result ?: 0L
    }

    data class RetailerOfferRow(
        val retailerProductId: UUID,
        val retailer: String,
        val retailerSku: String?,
        val url: String,
        val title: String,
        val price: BigDecimal,
        val originalPrice: BigDecimal?,
        val currency: String,
        val inStock: Boolean,
        val recordedAt: Instant,
    )

    fun findOffersForProduct(canonicalProductId: UUID): Flow<RetailerOfferRow> {
        val sql = """
            SELECT DISTINCT ON (rp.id)
                rp.id AS retailer_product_id,
                rp.retailer,
                rp.retailer_sku,
                rp.url,
                rp.title,
                ph.price,
                ph.original_price,
                ph.currency,
                ph.in_stock,
                ph.recorded_at
            FROM retailer_products rp
            JOIN price_history ph ON ph.retailer_product_id = rp.id
            WHERE rp.canonical_product_id = :canonicalProductId
            ORDER BY rp.id, ph.recorded_at DESC
        """.trimIndent()

        return client.sql(sql)
            .bind("canonicalProductId", canonicalProductId)
            .map { row, _ ->
                RetailerOfferRow(
                    retailerProductId = row.get("retailer_product_id", UUID::class.java)!!,
                    retailer = row.get("retailer", String::class.java)!!,
                    retailerSku = row.get("retailer_sku", String::class.java),
                    url = row.get("url", String::class.java)!!,
                    title = row.get("title", String::class.java)!!,
                    price = row.get("price", BigDecimal::class.java)!!,
                    originalPrice = row.get("original_price", BigDecimal::class.java),
                    currency = row.get("currency", String::class.java) ?: "USD",
                    inStock = row.get("in_stock", java.lang.Boolean::class.java)?.booleanValue() ?: true,
                    recordedAt = row.get("recorded_at", Instant::class.java) ?: Instant.now()
                )
            }.all().asFlow()
    }

    fun findHistoricalPriceBuckets(
        canonicalProductId: UUID,
        bucketInterval: String,
        since: Instant
    ): Flow<PriceHistoryPointDto> {
        val sql = """
            SELECT
                time_bucket(INTERVAL '$bucketInterval', recorded_at) AS bucket_ts,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price)::NUMERIC(12, 2) AS avg_price,
                COUNT(*) AS sample_count
            FROM price_history
            WHERE canonical_product_id = :canonicalProductId
              AND recorded_at >= :since
            GROUP BY bucket_ts
            ORDER BY bucket_ts ASC
        """.trimIndent()

        return client.sql(sql)
            .bind("canonicalProductId", canonicalProductId)
            .bind("since", since)
            .map { row, _ ->
                PriceHistoryPointDto(
                    bucketTimestamp = row.get("bucket_ts", Instant::class.java)!!,
                    minPrice = row.get("min_price", BigDecimal::class.java)!!,
                    maxPrice = row.get("max_price", BigDecimal::class.java)!!,
                    avgPrice = row.get("avg_price", BigDecimal::class.java)!!,
                    sampleCount = row.get("sample_count", java.lang.Long::class.java)?.toLong() ?: 0L
                )
            }.all().asFlow()
    }

    suspend fun findAllTimeLowAndHigh(canonicalProductId: UUID): Pair<BigDecimal?, BigDecimal?>? {
        val sql = """
            SELECT MIN(price) AS all_time_low, MAX(price) AS all_time_high
            FROM price_history
            WHERE canonical_product_id = :canonicalProductId
        """.trimIndent()

        return client.sql(sql)
            .bind("canonicalProductId", canonicalProductId)
            .map { row, _ ->
                val low = row.get("all_time_low", BigDecimal::class.java)
                val high = row.get("all_time_high", BigDecimal::class.java)
                Pair(low, high)
            }.awaitOneOrNull()
    }
}


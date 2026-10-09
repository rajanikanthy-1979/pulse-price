package com.pulseprice.pipeline.resolver

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.pipeline.domain.CanonicalProductEntity
import com.pulseprice.pipeline.domain.CanonicalProductRepository
import com.pulseprice.pipeline.domain.RetailerProductEntity
import com.pulseprice.pipeline.domain.RetailerProductRepository
import com.pulseprice.pipeline.model.CanonicalResolutionResult
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID
import kotlin.time.Duration.Companion.hours
import kotlin.time.toJavaDuration

@Service
class CanonicalResolver(
    private val gtinValidator: GtinValidator,
    private val canonicalProductRepository: CanonicalProductRepository,
    private val retailerProductRepository: RetailerProductRepository,
    private val redisTemplate: ReactiveStringRedisTemplate
) {

    suspend fun resolve(event: RawScrapeEvent): CanonicalResolutionResult {
        val payload = event.payload
        val normalizedGtin = gtinValidator.validateAndNormalize(payload.gtin)

        val canonicalProduct = if (normalizedGtin != null) {
            resolveByGtin(normalizedGtin, payload.title, payload.brand)
        } else {
            resolveByFallback(payload.title, payload.brand)
        }

        val retailProduct = resolveRetailerProduct(
            canonicalId = canonicalProduct.id!!,
            retailer = event.retailerDomain,
            url = event.url,
            sku = payload.sku,
            title = payload.title,
        )

        return CanonicalResolutionResult(canonicalProduct, retailProduct)
    }

    private suspend fun resolveByGtin(gtin: String, title: String, brand: String?): CanonicalProductEntity {
        val cacheKey = "$REDIS_GTIN_PREFIX$gtin"

        // STEP A: Check Redis L2 Cache
        val cachedIdStr = redisTemplate.opsForValue().get(cacheKey).awaitSingleOrNull()
        if (!cachedIdStr.isNullOrEmpty()) {
            val cacheId = UUID.fromString(cachedIdStr)
            val found = canonicalProductRepository.findById(cacheId)
            if (found != null) {
                return found
            }
        }

        // Step B: Query Database
        val existing = canonicalProductRepository.findByGtin(gtin)
        if (existing != null) {
            redisTemplate.opsForValue().set(cacheKey, existing.id.toString(), CACHE_TTL).awaitSingleOrNull()
            return existing
        }

        // Step C: Create a new canonical product
        val newEntity = CanonicalProductEntity(
            gtin = gtin,
            title = title,
            brand = brand,
            createdAt = Instant.now(),
            updatedAt = Instant.now(),
        )

        val saved = canonicalProductRepository.save(newEntity)
        return saved
    }

    private suspend fun resolveByFallback(title: String, brand: String?): CanonicalProductEntity {
        val existing = if (brand != null) {
            canonicalProductRepository.findByBrandAndTitle(brand, title)
        } else null

        if (existing != null) return existing

        val provisional = CanonicalProductEntity(
            gtin = null,
            title = title,
            brand = brand,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        return canonicalProductRepository.save(provisional)
    }

    private suspend fun resolveRetailerProduct(
        canonicalId: UUID,
        retailer: String,
        url: String,
        sku: String?,
        title: String
    ): RetailerProductEntity {
        val existing = retailerProductRepository.findByUrl(url)
        if (existing != null) {
            // Update title or SKU if changed
            if (existing.title != title || existing.retailerSku != sku) {
                return retailerProductRepository.save(
                    existing.copy(title = title, retailerSku = sku, updatedAt = Instant.now())
                )
            }
            return existing
        }

        val newRetailerProduct = RetailerProductEntity(
            canonicalProductId = canonicalId,
            retailer = retailer,
            retailerSku = sku,
            url = url,
            title = title,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        return retailerProductRepository.save(newRetailerProduct)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CanonicalResolver::class.java)
        private const val REDIS_GTIN_PREFIX = "cache:canonical:gtin:"
        private val CACHE_TTL = 24.hours.toJavaDuration()
    }
}

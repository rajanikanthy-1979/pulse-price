package com.pulseprice.api.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.api.domain.ProductSearchCustomRepository
import com.pulseprice.common.model.ProductSearchHit
import com.pulseprice.common.model.ProductSearchResult
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class MetasearchProductService(
    private val searchCustomRepository: ProductSearchCustomRepository,
    private val redisTemplate: ReactiveStringRedisTemplate,
    private val objectMapper: ObjectMapper,
    @Value("\${pulse.cache.search-ttl-seconds:60}")
    private val searchTtlSeconds: Long
) {

    suspend fun search(
        query: String,
        brand: String?,
        page: Int,
        size: Int
    ) : ProductSearchResult {
        val cacheKey = "cache:search:${query.trim().lowercase()}:${brand?.lowercase()}:$page:$size"
        val cached = redisTemplate.opsForValue().get(cacheKey).awaitSingleOrNull()
        if (cached != null) {
            try {
                return objectMapper.readValue(cached, ProductSearchResult::class.java)
            } catch (e: Exception) {
                log.warn("Cache deserialization failed for {} : {}", cacheKey, e.message)
            }
        }

        val offset = page * size
        val itemsFlow = searchCustomRepository.searchProducts(
            query = query.trim(),
            brand = brand?.trim(),
            limit = size,
            offset = offset
        )

        val hits = itemsFlow.map { row ->
            ProductSearchHit(
                id = row.id,
                gtin = row.gtin,
                brand = row.brand,
                model = row.model,
                title = row.title,
                lowestPrice = row.lowestPrice,
                highestPrice = row.highestPrice,
                currency = row.currency ?: "USD",
                retailerCount = row.retailerCount.toInt(),
                inStockCount = row.inStockCount.toInt(),
                similarityScore = row.similarityScore,
            )
        }.toList()

        val totalElements = searchCustomRepository.countSearchResults(query = query.trim(), brand = brand?.trim())
        val totalPages = if (size > 0) ((totalElements + size - 1) / size).toInt() else 0

        val result = ProductSearchResult(
            items = hits,
            page = page,
            size = size,
            totalElements = totalElements,
            totalPages = totalPages
        )

        try {
            val json = objectMapper.writeValueAsString(result)
            redisTemplate.opsForValue().set(cacheKey, json, Duration.ofSeconds(searchTtlSeconds)).awaitSingleOrNull()
        } catch (ex: Exception) {
            log.warn("Failed to write search results to redis cache: {}", ex.message)
        }
        return result
    }


    companion object {
        private val log = LoggerFactory.getLogger(MetasearchProductService::class.java)
    }
}

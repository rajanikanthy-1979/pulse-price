package com.pulseprice.pipeline.service

import com.pulseprice.common.event.CanonicalProductPriceEvent
import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.pipeline.domain.PriceHistoryRepository
import com.pulseprice.pipeline.model.CanonicalResolutionResult
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class PricePersistenceService(
    private val priceHistoryRepository: PriceHistoryRepository,
    @Qualifier("downstreamKafkaTemplate") private val kafkaTemplate: KafkaTemplate<String, Any>,
) {

    suspend fun recordPrice(resolution: CanonicalResolutionResult, scrapeEvent: RawScrapeEvent) {
        val payload = scrapeEvent.payload
        val recordedAt = Instant.now()

        priceHistoryRepository.insertPricePoint(
            recordedAt = recordedAt,
            retailerProductId = resolution.retailerProduct.id!!,
            canonicalProductId = resolution.canonicalProduct.id!!,
            price = payload.currentPrice,
            currency = payload.currency,
            inStock = payload.inStock,
            originalPrice = payload.originalPrice,
        )
        log.info(
            "Recorded price [{}] for canonicalId={} at retailer={}",
            payload.currentPrice,
            resolution.canonicalProduct.id,
            resolution.retailerProduct.retailer
        )

        // 2. Publish Canonical Price Event downstream for alerts and real-time indexing
        val downstreamEvent = CanonicalProductPriceEvent(
            canonicalProductId = resolution.canonicalProduct.id,
            retailerProductId = resolution.retailerProduct.id,
            retailer = resolution.retailerProduct.retailer,
            gtin = resolution.canonicalProduct.gtin,
            price = payload.currentPrice,
            originalPrice = payload.originalPrice,
            currency = payload.currency,
            inStock = payload.inStock,
            recordedAt = recordedAt
        )

        try {
            kafkaTemplate.send(
                TOPIC_CANONICAL_PRICES,
                resolution.canonicalProduct.id.toString(),
                downstreamEvent
            ).await()
        } catch (ex: Exception) {
            log.error("Failed to emit CanonicalProductPriceEvent downstream: {}", ex.message, ex)
            // Note: DB write succeeded; message can be retried or recovered
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(PricePersistenceService::class.java)
        const val TOPIC_CANONICAL_PRICES = "canonical-prices"
    }
}

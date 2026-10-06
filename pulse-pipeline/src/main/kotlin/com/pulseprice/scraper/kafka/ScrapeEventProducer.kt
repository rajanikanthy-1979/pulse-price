package com.pulseprice.scraper.kafka

import com.pulseprice.common.event.RawScrapeEvent
import kotlinx.coroutines.future.await
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class ScrapeEventProducer(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    suspend fun publishRawScrape(event: RawScrapeEvent) {
        val key = "${event.retailerDomain}:${event.url}"
        val executionResult = runCatching {
            val result = kafkaTemplate.send(TOPIC_RAW_SCRAPES, key, event).await()
            log.info(
                "Published RawScrapeEvent [id={}] to partition={} offset={}",
                event.eventId,
                result.recordMetadata.partition(),
                result.recordMetadata.offset()
            )
        }
        executionResult.onFailure { ex ->
            log.error("Failed to publish RawscrapeEvent [id={}] to Kafka: {}", event.eventId, ex.message, ex)
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(ScrapeEventProducer::class.java)
        const val TOPIC_RAW_SCRAPES = "raw-scrapes"
    }
}

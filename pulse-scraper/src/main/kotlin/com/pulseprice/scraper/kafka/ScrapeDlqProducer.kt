package com.pulseprice.scraper.kafka

import com.pulseprice.common.event.ScrapeDeadLetterEvent
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class ScrapeDlqProducer(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    suspend fun publishDeadLetter(event: ScrapeDeadLetterEvent) {
        val partitionKey = "${event.retailerDomain}:${event.failureReason}"

        val result = runCatching {
            val sendResult = kafkaTemplate.send(TOPIC_RAW_SCRAPES_DLQ, partitionKey, event).await()
            log.info(
                "Published ScrapeDeadLetterEvent [id={}] reason={} for url {} to partition={} offset={}",
                event.eventId,
                event.failureReason,
                event.url,
                sendResult.recordMetadata.partition(),
                sendResult.recordMetadata.offset()
            )
        }

        result.onFailure { ex ->
            log.error("Failed to publish ScrapeDeadLetterEvent [id={}] to Kafka: {}", event.eventId, ex.message)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScrapeDlqProducer::class.java)
        const val TOPIC_RAW_SCRAPES_DLQ = "raw-scrapes-dlq"
    }
}

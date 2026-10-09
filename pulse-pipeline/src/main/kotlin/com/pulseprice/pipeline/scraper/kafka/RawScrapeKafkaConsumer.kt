package com.pulseprice.pipeline.scraper.kafka

import com.pulseprice.common.event.RawScrapeEvent
import com.pulseprice.pipeline.resolver.CanonicalResolver
import com.pulseprice.pipeline.service.PricePersistenceService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class RawScrapeKafkaConsumer(
    private val canonicalResolver: CanonicalResolver,
    private val pricePersistenceService: PricePersistenceService,
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    companion object {
        private val log = LoggerFactory.getLogger(RawScrapeKafkaConsumer::class.java)
        const val TOPIC_DLT = "raw-scrapes-dlt"
    }

    private val scope = CoroutineScope(Dispatchers.IO)

    @KafkaListener(topics = ["raw-scrapes"], containerFactory = "kafkaListenerContainerFactory")
    fun onRawScrape(record: ConsumerRecord<String, RawScrapeEvent?>, acknowledgment: Acknowledgment) {
        val event = record.value()
        if (event == null) {
            log.warn("Received null/unparsable payload from partition={}, offset={}. Routing to DLT.", record.partition(), record.offset())
            kafkaTemplate.send(TOPIC_DLT, record.key() ?: "unknown", "CORRUPTED_EVENT")
            acknowledgment.acknowledge()
            return
        }
        scope.launch {
            try {
                // 1. Resolve to Canonical Product & Retailer Product
                val resolution = canonicalResolver.resolve(event)
                // 2. Persist to TimescaleDB Hypertable & emit downstream
                pricePersistenceService.recordPrice(resolution, event)
                acknowledgment.acknowledge()
            } catch (ex: Exception) {
                log.error("Failed to process event [id={}]: {}. Routing to DLT.", event.eventId, ex.message, ex)
                kafkaTemplate.send(TOPIC_DLT, record.key() ?: "unknown", event)
                acknowledgment.acknowledge()
            }
        }
    }
}

package com.pulseprice.pipeline.scraper.kafka

import com.pulseprice.common.event.ScrapeDeadLetterEvent
import com.pulseprice.pipeline.service.ScrapeFailureAuditService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class ScrapeDlqKafkaConsumer(
    private val auditService: ScrapeFailureAuditService
) {

    private val consumerScope = CoroutineScope(Dispatchers.IO)

    @KafkaListener(
        topics = ["raw-scrapes-dlq"],
        groupId = "pulse-dlq-audit-group",
        containerFactory = "dlqKafkaListenerContainerFactory"
    )
    fun onMessage(record: ConsumerRecord<String, ScrapeDeadLetterEvent?>, acknowledgment: Acknowledgment) {
        val event = record.value()
        if (event == null) {
            log.error("Unparsable ScrapeDeadLetterEvent at partition={}, offset={}. Skipping.", record.partition(), record.offset())
            acknowledgment.acknowledge()
            return
        }
        log.warn(
            "DLQ Alert: Scrape failure detected on domain [{}] url [{}]. Reason: {}. Tiers: {}",
            event.retailerDomain,
            event.url,
            event.failureReason,
            event.attemptedTiers
        )

        consumerScope.launch {
            try {
                auditService.recordFailure(event)
                acknowledgment.acknowledge()
            } catch (ex: Exception) {
                log.error("Error processing ScrapeDeadLetterEvent [id={}]: {}", event.eventId, ex.message, ex)
                // Acknowledge to prevent poison pill loop
                acknowledgment.acknowledge()
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(ScrapeDlqKafkaConsumer::class.java)
    }
}

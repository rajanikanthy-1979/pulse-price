package com.pulseprice.api.alert

import com.pulseprice.common.event.CanonicalProductPriceEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class AlertKafkaConsumer(
    private val alertEvaluationEngine: AlertEvaluationEngine
) {

    private val consumerScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(
        topics = ["\${pulse.kafka.topics.canonical-prices:canonical-prices}"],
        groupId = "\${spring.kafka.consumer.group-id:pulse-alert-engine-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onCanonicalPriceReceived(
        record: ConsumerRecord<String, CanonicalProductPriceEvent>,
        ack: Acknowledgment?
    ) {
        val event = record.value()
        logger.debug(
            "Alert engine received canonical price event: canonicalId={} price={}",
            event.canonicalProductId,
            event.price
        )
        consumerScope.launch {
            try {
                alertEvaluationEngine.evaluatePriceEvent(event)
                ack?.acknowledge()
            } catch (e: Exception) {
                logger.error("Error evaluating alert for canonicalId = {}: {}", event.canonicalProductId, e.message)
                ack?.acknowledge()
            }
        }
    }


    companion object {
        private val logger = LoggerFactory.getLogger(AlertKafkaConsumer::class.java)
    }
}

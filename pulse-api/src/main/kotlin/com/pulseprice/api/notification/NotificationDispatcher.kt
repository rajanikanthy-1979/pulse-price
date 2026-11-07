package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class NotificationDispatcher(channelHandlers: List<NotificationChannelHandler>) {
    private val handlerMap: Map<NotificationChannel, NotificationChannelHandler> =
        channelHandlers.associateBy { it.supportedChannel }

    private val dispatchScope = CoroutineScope(Dispatchers.IO)

    @KafkaListener(
        topics = ["\${pulse.kafka.topics.price-drop-alerts:price-drop-alerts}"],
        groupId = "\${pulse.kafka.group-id:pulse-notification-dispatcher-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun onPriceDropAlertReceived(
        record: ConsumerRecord<String, PriceDropAlertEvent>,
        ack: Acknowledgment?
    ) {
        val event = record.value()
        log.info("Received PriceDropAlertEvent: id={} channel={} destination={}", event.eventId, event.channel, event.destination)

        dispatchScope.launch {
            val handler = handlerMap[event.channel]
            if (handler == null) {
                log.warn("No registered handler for notification channel: {}", event.channel)
                ack?.acknowledge()
                return@launch
            }
            try {
                val delivered = handler.sendNotification(event)
                log.info("Notification delivery result for event {}: {}", event.eventId, delivered)
                ack?.acknowledge()
                return@launch
            } catch (ex: Exception) {
                log.error("Failed to execute notification handler for event {} : {}", event.eventId, ex.message, ex)
                ack?.acknowledge()
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(SseNotificationHandler::class.java)
    }
}

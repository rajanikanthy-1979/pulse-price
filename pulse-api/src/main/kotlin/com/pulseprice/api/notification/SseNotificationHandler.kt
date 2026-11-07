package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class SseNotificationHandler : NotificationChannelHandler {
    override val supportedChannel: NotificationChannel = NotificationChannel.SSE;

    private val _alertFlow = MutableSharedFlow<PriceDropAlertEvent>(extraBufferCapacity = 64)
    val alertFlow: SharedFlow<PriceDropAlertEvent> = _alertFlow.asSharedFlow()

    override suspend fun sendNotification(event: PriceDropAlertEvent): Boolean {
        val emitted = _alertFlow.tryEmit(event)
        log.debug("SSE Alert emitted for product {}: success {}", event, emitted)
        return emitted
    }

    companion object {
        private val log = LoggerFactory.getLogger(SseNotificationHandler::class.java)
    }
}

package com.pulseprice.api.controller

import com.pulseprice.api.notification.SseNotificationHandler
import com.pulseprice.common.model.PriceStreamUpdate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.util.UUID

@RestController
@RequestMapping(value = ["/api/v1/products"])
class PriceStreamController(
    private val sseNotificationHandler: SseNotificationHandler
) {
    @GetMapping("/{id}/live-stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamProductPrices(@PathVariable id: UUID): Flow<ServerSentEvent<PriceStreamUpdate>> {
        return sseNotificationHandler.alertFlow
            .filter { it.canonicalProductId == id }
            .map { alert ->
                ServerSentEvent.builder<PriceStreamUpdate>()
                    .id(alert.eventId)
                    .event("price-drop")
                    .data(
                        PriceStreamUpdate(
                            canonicalProductId = alert.canonicalProductId,
                            retailerProductId = alert.retailerProductId,
                            retailer = alert.retailer,
                            price = alert.currentPrice,
                            originalPrice = alert.previousPrice,
                            currency = alert.currency,
                            inStock = true,
                            recordedAt = alert.triggeredAt
                        )
                    ).retry(Duration.ofSeconds(5))
                    .build()
            }
    }
}

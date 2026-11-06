package com.pulseprice.api.alert

import com.pulseprice.api.domain.PriceAlertHistoryRepository
import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.AlertStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class AlertHistoryService(
    private val alertHistoryRepository: PriceAlertHistoryRepository
) {

    private val scope = CoroutineScope(Dispatchers.IO)

    fun recordAlertDispatched(event: PriceDropAlertEvent) {
        scope.launch {
            try {
                alertHistoryRepository.insertAuditRecord(
                    id = UUID.fromString(event.eventId),
                    subscriptionId = event.subscriptionId,
                    canonicalProductId = event.canonicalProductId,
                    retailerProductId = event.retailerProductId,
                    triggeredAt = event.triggeredAt,
                    triggerType = event.alertType.name,
                    oldPrice = event.previousPrice,
                    newPrice = event.currentPrice,
                    priceDropAmount = event.priceDropAmount,
                    priceDropPercentage = event.priceDropPercentage,
                    channel = event.channel.name,
                    destination = event.destination,
                    status = AlertStatus.PENDING.name,
                    errorMessage = null
                )
            } catch (ex: Exception) {
                log.error("Failed to insert alert audit record for eventId= {}: {}", event.eventId, ex.message)
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(AlertHistoryService::class.java)
    }
}

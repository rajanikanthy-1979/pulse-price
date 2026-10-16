package com.pulseprice.common.event

import com.pulseprice.common.model.AlertType
import com.pulseprice.common.model.NotificationChannel
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class PriceDropAlertEvent (
    val eventId: String = UUID.randomUUID().toString(),
    val subscriptionId: UUID,
    val canonicalProductId: UUID,
    val retailerProductId: UUID,
    val productTitle: String,
    val retailer: String,
    val productUrl: String,
    val currentPrice: BigDecimal,
    val previousPrice: BigDecimal,
    val priceDropAmount: BigDecimal,
    val priceDropPercentage: BigDecimal,
    val currency: String,
    val alertType: AlertType,
    val channel: NotificationChannel,
    val destination: String,
    val triggeredAt: Instant = Instant.now()
)

package com.pulseprice.common.model

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

enum class AlertType {
    TARGET_PRICE,
    PERCENTAGE_DROP,
    ALL_TIME_LOW,
    BACK_IN_STOCK
}

enum class NotificationChannel {
    EMAIL,
    WEBHOOK,
    SSE
}

enum class AlertStatus {
    PENDING,
    DELIVERED,
    FAILED,
    COOLDOWN_SUPPRESSED
}

data class PriceAlertSubscriptionRequest (
    val canonicalProductId: UUID,
    val userEmail: String,
    val alertType: AlertType,
    val targetPrice: BigDecimal? = null,
    val percentageDropThreshold: BigDecimal? = null,
    val notificationChannel: NotificationChannel = NotificationChannel.EMAIL,
    val destination: String,
    val cooldownHours: Int = 24
)

data class PriceAlertSubscriptionResponse (
    val id: UUID,
    val canonicalProductId: UUID,
    val userEmail: String,
    val alertType: AlertType,
    val targetPrice: BigDecimal?,
    val percentageDropThreshold: BigDecimal?,
    val notificationChannel: NotificationChannel,
    val destination: String,
    val active: Boolean,
    val cooldownHours: Int,
    val lastTriggeredAt: Instant?,
    val createdAt: Instant,
    val updatedAt: Instant
)

package com.pulseprice.api.domain

import com.pulseprice.common.model.AlertStatus
import com.pulseprice.common.model.AlertType
import com.pulseprice.common.model.NotificationChannel
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

@Table("canonical_products")
data class CanonicalProductEntity(
    @Id val id: UUID? = null,
    val gtin: String?,
    val brand: String?,
    val model: String?,
    val title: String,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now(),
)

@Table("retailer_products")
data class RetailerProductEntity(
    @Id val id: UUID? = null,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    val retailer: String,
    @Column("retailer_sku") val retailerSku: String?,
    val url: String,
    val title: String,
    @Column("create_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now(),
)

@Table("price_history")
data class PriceHistoryEntity(
    @Column("recorded_at") val recordedAt: Instant = Instant.now(),
    @Column("retailer_product_id") val retailerProductId: UUID,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    val price: BigDecimal,
    @Column("original_price") val originalPrice: BigDecimal?,
    val currency: String = "USD",
    @Column("in_stock") val inStock: Boolean = true
)

@Table("price_alert_subscriptions")
data class PriceAlertSubscriptionEntity(
    @Id val id: UUID? = null,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    @Column("user_email") val userEmail: String,
    @Column("alert_type") val alertType: AlertType,
    @Column("target_price") val targetPrice: BigDecimal?,
    @Column("percentage_drop_threshold") val percentageDropThreshold: BigDecimal?,
    @Column("notification_channel") val notificationChannel: NotificationChannel,
    val destination: String,
    val active: Boolean = true,
    @Column("cooldown_hours") val cooldownHours: Int = 24,
    @Column("last_triggered_at") val lastTriggeredAt: Instant? = null,
    @Column("created_at") val createdAt: Instant = Instant.now(),
    @Column("updated_at") val updatedAt: Instant = Instant.now()
)

@Table("price_alert_history")
data class PriceAlertHistoryEntity(
    val id: UUID = UUID.randomUUID(),
    @Column("subscription_id") val subscriptionId: UUID,
    @Column("canonical_product_id") val canonicalProductId: UUID,
    @Column("retailer_product_id") val retailerProductId: UUID,
    @Column("triggered_at") val triggeredAt: Instant = Instant.now(),
    @Column("trigger_type") val triggerType: AlertType,
    @Column("old_price") val oldPrice: BigDecimal?,
    @Column("new_price") val newPrice: BigDecimal,
    @Column("price_drop_amount") val priceDropAmount: BigDecimal?,
    @Column("price_drop_percentage") val priceDropPercentage: BigDecimal?,
    val channel: NotificationChannel,
    val destination: String,
    val status: AlertStatus = AlertStatus.PENDING,
    @Column("error_message") val errorMessage: String? = null
)

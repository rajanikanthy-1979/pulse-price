package com.pulseprice.api.alert

import com.pulseprice.api.domain.CanonicalProductRepository
import com.pulseprice.api.domain.PriceAlertSubscriptionEntity
import com.pulseprice.api.domain.PriceAlertSubscriptionRepository
import com.pulseprice.api.domain.ProductSearchCustomRepository
import com.pulseprice.api.domain.RetailProductRepository
import com.pulseprice.common.event.CanonicalProductPriceEvent
import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.AlertType
import kotlinx.coroutines.flow.toList
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant


@Service
class AlertEvaluationEngine(
    private val subscriptionRepository: PriceAlertSubscriptionRepository,
    private val canonicalProductRepository: CanonicalProductRepository,
    private val retailProductRepository: RetailProductRepository,
    private val searchCustomRepository: ProductSearchCustomRepository,
    private val cooldownManager: AlertCooldownManager,
    private val alertHistoryService: AlertHistoryService,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    @Value("\${pulse.kafka.topics.price-drop-alerts:price-drop-alerts}")
    private val priceDropAlertsTopic: String,
) {

    suspend fun evaluatePriceEvent(event: CanonicalProductPriceEvent) {
        val canonicalId = event.canonicalProductId
        val currentPrice = event.price

        val previousPrice = cooldownManager.getPreviousPrice(event.retailerProductId)
        cooldownManager.recordLatestPrice(event.retailerProductId, currentPrice)

        val subscriptions = subscriptionRepository.findByCanonicalProductId(canonicalId).toList()

        if (subscriptions.isEmpty()) {
            log.trace("No active subscriptions for canonicalProductId={}", canonicalId)
            return
        }

        val product = canonicalProductRepository.findById(canonicalId)
        val retailerProduct = retailProductRepository.findById(event.retailerProductId)
        val productTitle = product?.title ?: retailerProduct?.title ?: "Tracked Product"
        val productUrl = retailerProduct?.url ?: ""

        val bounds = searchCustomRepository.findAllTimeLowAndHigh(canonicalId)
        val allTimeLow = bounds?.first

        for (sub in subscriptions) {
            val isTriggered = checkTriggerCondition(
                subscription = sub,
                currentPrice = currentPrice,
                previousPrice = previousPrice,
                allTimeLow = allTimeLow,
                inStock = event.inStock
            )

            if (isTriggered) {
                val lockAcquired = cooldownManager.acquireCooldownLock(
                    subscriptionId = sub.id!!,
                    canonicalProductId = canonicalId,
                    cooldownHours = sub.cooldownHours
                )

                if (!lockAcquired) {
                    log.info(
                        "Alert trigger suppressed by cooldown for subscriptionId={} user={}",
                        sub.id,
                        sub.userEmail
                    )
                    continue
                }

                // Compute drop metrics
                val dropAmount = if (previousPrice != null && previousPrice > currentPrice) {
                    previousPrice.subtract(currentPrice)
                } else BigDecimal.ZERO

                val dropPercentage =
                    if (previousPrice != null && previousPrice > BigDecimal.ZERO && previousPrice > currentPrice) {
                        dropAmount.divide(previousPrice, 4, RoundingMode.HALF_UP).multiply(BigDecimal(100))
                            .setScale(2, RoundingMode.HALF_UP)
                    } else BigDecimal.ZERO

                val alertEvent = PriceDropAlertEvent(
                    subscriptionId = sub.id,
                    canonicalProductId = canonicalId,
                    retailerProductId = event.retailerProductId,
                    productTitle = productTitle,
                    retailer = event.retailer,
                    productUrl = productUrl,
                    currentPrice = currentPrice,
                    previousPrice = previousPrice ?: currentPrice,
                    priceDropAmount = dropAmount,
                    priceDropPercentage = dropPercentage,
                    currency = event.currency,
                    alertType = sub.alertType,
                    channel = sub.notificationChannel,
                    destination = sub.destination,
                    triggeredAt = Instant.now()
                )

                // Dispatch to Kafka topic
                kafkaTemplate.send(priceDropAlertsTopic, sub.id.toString(), alertEvent)
                log.info(
                    "Dispatched PriceDropAlertEvent for subId={} type={} target={}",
                    sub.id,
                    sub.alertType,
                    sub.destination
                )

                // Update last_triggered_at in DB
                subscriptionRepository.updateLastTriggered(sub.id, Instant.now())

                // Record audit log in TimescaleDB
                alertHistoryService.recordAlertDispatched(alertEvent)

            }
        }
    }

    private fun checkTriggerCondition(
        subscription: PriceAlertSubscriptionEntity,
        currentPrice: BigDecimal,
        previousPrice: BigDecimal?,
        allTimeLow: BigDecimal?,
        inStock: Boolean
    ): Boolean {
        if (!inStock) return false

        return when (subscription.alertType) {
            AlertType.TARGET_PRICE -> {
                val target = subscription.targetPrice ?: return false
                currentPrice <= target
            }

            AlertType.PERCENTAGE_DROP -> {
                val threshold = subscription.percentageDropThreshold ?: return false
                if (previousPrice == null || previousPrice <= BigDecimal.ZERO || currentPrice >= previousPrice) {
                    return false
                }
                val dropPct = previousPrice.subtract(currentPrice)
                    .divide(previousPrice, 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal(100))

                dropPct >= threshold
            }

            AlertType.ALL_TIME_LOW -> {
                allTimeLow != null && currentPrice < allTimeLow
            }

            AlertType.BACK_IN_STOCK -> {
                true
            }
        }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(AlertEvaluationEngine::class.java)
    }
}

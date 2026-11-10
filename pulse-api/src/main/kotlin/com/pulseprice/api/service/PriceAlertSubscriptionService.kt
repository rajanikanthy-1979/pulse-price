package com.pulseprice.api.service

import com.pulseprice.api.domain.PriceAlertSubscriptionEntity
import com.pulseprice.api.domain.PriceAlertSubscriptionRepository
import com.pulseprice.common.model.PriceAlertSubscriptionRequest
import com.pulseprice.common.model.PriceAlertSubscriptionResponse
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID

@Service
class PriceAlertSubscriptionService(
    private val subscriptionRepository: PriceAlertSubscriptionRepository
) {

    suspend fun createSubscription(request: PriceAlertSubscriptionRequest): PriceAlertSubscriptionResponse {
        val entity = PriceAlertSubscriptionEntity(
            canonicalProductId = request.canonicalProductId,
            userEmail = request.userEmail,
            alertType = request.alertType,
            targetPrice = request.targetPrice,
            percentageDropThreshold = request.percentageDropThreshold,
            notificationChannel = request.notificationChannel,
            destination = request.destination,
            cooldownHours = request.cooldownHours,
        )

        val saved = subscriptionRepository.save(entity)
        return toDto(saved)
    }

    suspend fun deactivateSubscription(subscriptionId: UUID): Boolean {
        val existing = subscriptionRepository.findById(subscriptionId) ?: return false
        val updated = existing.copy(active = false, updatedAt = Instant.now())
        subscriptionRepository.save(updated)
        return true
    }

    suspend fun getSubscriptionsForUser(userEmail: String): List<PriceAlertSubscriptionResponse> {
        return subscriptionRepository.findByUserEmail(userEmail)
            .map { toDto(it) }
            .toList()
    }


    private fun toDto(entity: PriceAlertSubscriptionEntity): PriceAlertSubscriptionResponse {
        return PriceAlertSubscriptionResponse(
            id = entity.id!!,
            canonicalProductId = entity.canonicalProductId,
            userEmail = entity.userEmail,
            alertType = entity.alertType,
            targetPrice = entity.targetPrice,
            percentageDropThreshold = entity.percentageDropThreshold,
            notificationChannel = entity.notificationChannel,
            destination = entity.destination,
            active = entity.active,
            cooldownHours = entity.cooldownHours,
            lastTriggeredAt = entity.lastTriggeredAt,
            createdAt = entity.createdAt,
            updatedAt = entity.updatedAt
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(PriceAlertSubscriptionService::class.java)
    }
}

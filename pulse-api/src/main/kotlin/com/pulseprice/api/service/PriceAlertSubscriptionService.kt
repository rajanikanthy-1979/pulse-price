package com.pulseprice.api.service

import com.pulseprice.api.domain.PriceAlertSubscriptionEntity
import com.pulseprice.api.domain.PriceAlertSubscriptionRepository
import com.pulseprice.common.model.PriceAlertSubscriptionRequest
import com.pulseprice.common.model.PriceAlertSubscriptionResponse
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

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

    private fun toDto(entity: PriceAlertSubscriptionEntity): PriceAlertSubscriptionResponse {
        TODO()
    }

    companion object {
        private val log = LoggerFactory.getLogger(PriceAlertSubscriptionService::class.java)
    }
}

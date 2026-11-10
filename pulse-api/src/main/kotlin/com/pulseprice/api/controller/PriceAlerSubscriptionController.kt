package com.pulseprice.api.controller

import com.pulseprice.api.service.PriceAlertSubscriptionService
import com.pulseprice.common.model.PriceAlertSubscriptionRequest
import com.pulseprice.common.model.PriceAlertSubscriptionResponse
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.util.UUID

@RestController
@RequestMapping("/api/v1/alerts/subscriptions")
class PriceAlertSubscriptionController(
    private val subscriptionService: PriceAlertSubscriptionService
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    suspend fun createSubscription(
        @Valid @RequestBody request: PriceAlertSubscriptionRequest
    ): PriceAlertSubscriptionResponse {
        return subscriptionService.createSubscription(request)
    }

    @GetMapping
    suspend fun getSubscriptions(
        @RequestParam userEmail: String
    ): List<PriceAlertSubscriptionResponse> {
        return subscriptionService.getSubscriptionsForUser(userEmail)
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    suspend fun deleteSubscription(@PathVariable id: UUID) {
        val deactivated = subscriptionService.deactivateSubscription(id)
        if (!deactivated) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Subscription not found with id: $id")
        }
    }
}

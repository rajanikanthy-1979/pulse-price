package com.pulseprice.api.alert

import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Duration
import java.util.*

@Component
class AlertCooldownManager(private val redisTemplate: ReactiveStringRedisTemplate) {
    suspend fun acquireCooldownLock(
        subscriptionId: UUID,
        canonicalProductId: UUID,
        cooldownHours: Int
    ): Boolean {
        val key = "cooldown:alert:$subscriptionId:$canonicalProductId"
        val acquired = redisTemplate.opsForValue()
            .setIfAbsent(key, "ACTIVE", Duration.ofHours(cooldownHours.toLong()))
            .awaitFirstOrNull() ?: false

        if (!acquired) {
            log.debug(
                "Cooldown active for subscriptionId = {}, productId = {} suppressing alert",
                subscriptionId,
                canonicalProductId
            )
        }
        return acquired
    }

    suspend fun getPreviousPrice(retailerProductId: UUID): BigDecimal? {
        val key = "price:last:$retailerProductId"
        val raw = redisTemplate.opsForValue()
            .get(key).awaitFirstOrNull() ?: return null
        return try {
            BigDecimal(raw)
        } catch (ex: Exception) {
            null
        }
    }

    suspend fun recordLatestPrice(retailerProductId: UUID, price: BigDecimal) {
        val key = "price:last:$retailerProductId"
        redisTemplate.opsForValue().set(key, price.toPlainString(), Duration.ofDays(7)).awaitFirstOrNull()
    }

    companion object {
        private val log = LoggerFactory.getLogger(AlertCooldownManager::class.java)
    }
}

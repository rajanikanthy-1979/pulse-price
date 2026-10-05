package com.pulseprice.scraper.ratelimit

import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class RedisTokenBucketRateLimiter(
    private val redisTemplate: ReactiveStringRedisTemplate,
    private val redisScript: RedisScript<Long>
) {
    suspend fun tryAcquire(
        domain: String,
        capacity: Long = 5,
        refillRatePerSec: Long = 2,
        requested: Long = 1
    ): Boolean {
        val key = "ratelimit:$domain"
        val nowSec = Instant.now().epochSecond
        val result = redisTemplate.execute(
            redisScript,
            listOf(key),
            listOf(capacity.toString(), refillRatePerSec.toString(), requested.toString())
        ).awaitSingle()
        return result == 1L
    }
}
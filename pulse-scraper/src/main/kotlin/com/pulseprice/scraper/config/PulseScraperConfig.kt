package com.pulseprice.scraper.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.data.redis.core.script.RedisScript

@Configuration
class PulseScraperConfig {
    @Bean
    fun redisScript(): RedisScript<Long> {
        val luaScript = ClassPathResource("/rate_limiter.lua")
        return DefaultRedisScript(luaScript.file.readText(Charsets.UTF_8), Long::class.java)
    }
}
package com.pulseprice.scraper

import com.pulseprice.pipeline.scraper.kafka.KafkaProducerConfig
import com.pulseprice.pipeline.scraper.kafka.ScrapeEventProducer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.data.r2dbc.R2dbcDataAutoConfiguration
import org.springframework.boot.autoconfigure.data.r2dbc.R2dbcRepositoriesAutoConfiguration
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.web.reactive.function.client.WebClient

// pulse-pipeline drags R2DBC onto the classpath; the scraper has no database.
@SpringBootApplication(
    exclude = [R2dbcAutoConfiguration::class, R2dbcDataAutoConfiguration::class, R2dbcRepositoriesAutoConfiguration::class]
)
@Import(KafkaProducerConfig::class, ScrapeEventProducer::class)
class ScraperApplication {
    @Bean
    fun webClient(): WebClient = WebClient.builder()
        // Retailer product pages routinely exceed the 256 KB default buffer.
        .codecs { it.defaultCodecs().maxInMemorySize(10 * 1024 * 1024) }
        .build()
}

fun main(args: Array<String>) {
    runApplication<ScraperApplication>(*args)
}

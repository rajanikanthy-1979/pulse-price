package com.pulseprice.scraper

import com.pulseprice.pipeline.scraper.kafka.KafkaProducerConfig
import com.pulseprice.pipeline.scraper.kafka.ScrapeEventProducer
import com.pulseprice.scraper.service.ScrapeIngestionService
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
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

    @Bean
    fun scrapeOnStartup(scrapeIngestionService: ScrapeIngestionService) = CommandLineRunner {
        runBlocking {
            val result = scrapeIngestionService.scrapeAndIngest("https://www.walmart.com/ip/Mars-Mixed-Skittles-Starburst-Starburst-FaveREDS-More-Halloween-Candy-20-Ct-Bulk-Bag/5191505248?athAsset=eyJhdGhjcGlkIjoiNTE5MTUwNTI0OCIsImF0aHN0aWQiOiJDUzAyMCIsImdycElkIjoiMmRhMjFiZTYtMzQ3My00ZTRlLWI2ZTQtZWI2ODdkZjlmNWIwIiwiYXRoYW5jaWQiOiI1MTkxNTA1MjQ4IiwiYXRocmsiOjAuMH0=&athena=true")
            log.info("Result: $result")
        }
    }

     companion object {
         private val log: Logger = LoggerFactory.getLogger(ScraperApplication::class.java)
     }
}

fun main(args: Array<String>) {
    runApplication<ScraperApplication>(*args)
}

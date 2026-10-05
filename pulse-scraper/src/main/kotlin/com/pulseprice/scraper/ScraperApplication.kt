package com.pulseprice.scraper

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.web.reactive.function.client.WebClient

@SpringBootApplication
class ScraperApplication {
    @Bean
    fun webClient(): WebClient = WebClient.builder().build()
}

fun main(args: Array<String>) {
    runApplication<ScraperApplication>(*args)
}
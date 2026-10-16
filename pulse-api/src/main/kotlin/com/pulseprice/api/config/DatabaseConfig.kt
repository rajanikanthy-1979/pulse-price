package com.pulseprice.api.config

import io.r2dbc.spi.ConnectionFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.ReactiveTransactionManager

@Configuration
@EnableR2dbcRepositories(basePackages = ["com.pulseprice.api.domain"])
class DatabaseConfig(private val connectionFactory: ConnectionFactory): AbstractR2dbcConfiguration() {
    override fun connectionFactory(): ConnectionFactory = connectionFactory

    @Bean
    fun transactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager {
        return R2dbcTransactionManager(connectionFactory)
    }
}

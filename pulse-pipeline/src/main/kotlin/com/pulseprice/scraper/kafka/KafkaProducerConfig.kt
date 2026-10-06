package com.pulseprice.scraper.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaProducerConfig(
    @Value("\${spring.kafka.bootstrap-servers:localhost:9092}") private val bootStrapServers: String,
    private val objectMapper: ObjectMapper
) {
    @Bean
    fun producerFactory(): ProducerFactory<String, Any> {
       val configProps = mutableMapOf<String, Any> (
           ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootStrapServers,
           ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
           ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
           ProducerConfig.ACKS_CONFIG to "all",
           ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
           ProducerConfig.RETRIES_CONFIG to 3,
           ProducerConfig.LINGER_MS_CONFIG to 10,
           ProducerConfig.BATCH_SIZE_CONFIG to 32768,
           JsonSerializer.ADD_TYPE_INFO_HEADERS to false
       )
       val valueSerializer = JsonSerializer<Any>(objectMapper)
       return DefaultKafkaProducerFactory(configProps, StringSerializer(), valueSerializer)
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, Any> = KafkaTemplate(producerFactory())
}

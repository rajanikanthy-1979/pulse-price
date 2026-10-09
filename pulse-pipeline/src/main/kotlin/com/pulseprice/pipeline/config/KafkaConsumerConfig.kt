package com.pulseprice.pipeline.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.event.RawScrapeEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer

@EnableKafka
@Configuration
class KafkaConsumerConfig(
    @Value("\${spring.kafka.bootstrap-servers:localhost:9092}")
    private val bootstrapServers: String,
    private val objectMapper: ObjectMapper
) {
    @Bean
    fun consumerFactory(): ConsumerFactory<String, RawScrapeEvent> {

        val props = mutableMapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "pulse-pipeline-group",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
        val jsonDeserializer = JsonDeserializer(RawScrapeEvent::class.java, objectMapper)
        jsonDeserializer.ignoreTypeHeaders()
        jsonDeserializer.trustedPackages("com.pulseprice.*")
        return DefaultKafkaConsumerFactory(props, StringDeserializer(), ErrorHandlingDeserializer(jsonDeserializer))
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, RawScrapeEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, RawScrapeEvent>()
        factory.consumerFactory = consumerFactory()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        factory.setConcurrency(3)
        return factory
    }

    @Bean
    fun downstreamProducerFactory(): ProducerFactory<String, Any> {
        val props = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            JsonSerializer.ADD_TYPE_INFO_HEADERS to false
        )
        return DefaultKafkaProducerFactory(props, StringSerializer(), JsonSerializer(objectMapper))
    }

    @Bean
    fun downstreamKafkaTemplate(): KafkaTemplate<String, Any> = KafkaTemplate(downstreamProducerFactory())
}

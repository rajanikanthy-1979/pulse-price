package com.pulseprice.api.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class WehhookNotificationHandler(
    private val webClientBuilder: WebClient.Builder,
    private val objectMapper: ObjectMapper,
    @Value("\${pulse.alerts.hmac-secret:pulse-engine-super-secret-key-32bytes!}")
    private val hmacSecret: String,
    @Value("\${pulse.alerts.webhook-timeout-ms:4000}")
    private val timeoutMs: Long
): NotificationChannelHandler {
    override val supportedChannel: NotificationChannel = NotificationChannel.WEBHOOK
    private val client = webClientBuilder.build()


    override suspend fun sendNotification(event: PriceDropAlertEvent): Boolean {
        val destinationUrl = event.destination
        val jsonPayload = objectMapper.writeValueAsString(event)
        val timestamp = System.currentTimeMillis().toString()
        val signature = calculateHmacSha256("$timestamp$jsonPayload", hmacSecret)

        return try {
            val responseStatus = client.post()
                .uri(destinationUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Pulse-Timestamp", timestamp)
                .header("X-Pulse-Signature", "sha256=$signature")
                .bodyValue(jsonPayload)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofMillis(timeoutMs))
                .awaitFirstOrNull()

            val success = responseStatus?.statusCode?.is2xxSuccessful == true
            if (success) {
                log.info("Webhook successfully dispatched to {} for eventId = {}", destinationUrl, event.eventId)
            } else {
                log.warn("Webhook dispatched to {} returned non-2xx status: {}", destinationUrl, responseStatus?.statusCode)
            }
            success

        } catch (ex: Exception) {
            log.error("Failed to deliver webhook to {}: {}", destinationUrl, ex.message)
            false
        }
    }

    private fun calculateHmacSha256(data: String, key: String): String {
        val sha256Hmac = Mac.getInstance("HmacSHA256")
        val secretKey = SecretKeySpec(key.toByteArray(Charsets.UTF_8), "HmacSHA256")
        sha256Hmac.init(secretKey)
        val hash = sha256Hmac.doFinal(data.toByteArray(Charsets.UTF_8))
        return hash.joinToString("") { "%02x".format(it) }
    }

    companion object {
        private val log: Logger = LoggerFactory.getLogger(WehhookNotificationHandler::class.java)
    }
}

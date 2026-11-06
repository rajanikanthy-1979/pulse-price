package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel
import org.slf4j.LoggerFactory

class EmailNotificationHandler: NotificationChannelHandler {
    override val supportedChannel: NotificationChannel = NotificationChannel.EMAIL

    override suspend fun sendNotification(event: PriceDropAlertEvent): Boolean {
        val recipient = event.destination
        val htmlContent = buildEmailTemplate(event)

        log.info(
            "Sending Price Drop Email to [{}] | Product: '{}' | Dropped from \${} to \${} ({}% OFF) | Deal URL: {}",
            recipient,
            event.productTitle,
            event.previousPrice,
            event.currentPrice,
            event.priceDropPercentage,
            event.productUrl
        )
        log.debug("Email HTML preview for [{}]:\n{}", recipient, htmlContent)
        return true
    }

    private fun buildEmailTemplate(event: PriceDropAlertEvent): String {
        return """
            <!DOCTYPE html>
            <html>
            <head><meta charset="utf-8"><style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f6f8; margin: 0; padding: 20px; }
                .card { max-width: 600px; margin: auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
                .header { background: #1e293b; color: #ffffff; padding: 24px; text-align: center; }
                .content { padding: 32px 24px; }
                .badge { display: inline-block; background: #10b981; color: white; padding: 4px 12px; border-radius: 9999px; font-weight: bold; font-size: 14px; }
                .price-box { margin: 24px 0; padding: 16px; background: #f8fafc; border-left: 4px solid #10b981; border-radius: 4px; }
                .current-price { font-size: 28px; font-weight: 800; color: #0f172a; }
                .old-price { text-decoration: line-through; color: #64748b; font-size: 18px; margin-left: 12px; }
                .btn { display: inline-block; padding: 14px 28px; background: #2563eb; color: white; text-decoration: none; border-radius: 6px; font-weight: 600; margin-top: 16px; }
            </style></head>
            <body>
                <div class="card">
                    <div class="header"><h2>PulsePrice Alert: Price Drop Detected!</h2></div>
                    <div class="content">
                        <span class="badge">${event.priceDropPercentage}% PRICE DROP</span>
                        <h3>${event.productTitle}</h3>
                        <p>Tracked at retailer: <strong>${event.retailer}</strong></p>
                        <div class="price-box">
                            <span class="current-price">$${event.currentPrice} ${event.currency}</span>
                            <span class="old-price">$${event.previousPrice}</span>
                        </div>
                        <a href="${event.productUrl}" class="btn" target="_blank">View Deal at ${event.retailer} →</a>
                    </div>
                </div>
            </body>
            </html>
        """.trimIndent()
    }

    companion object {
        private val log = LoggerFactory.getLogger(EmailNotificationHandler::class.java)
    }
}

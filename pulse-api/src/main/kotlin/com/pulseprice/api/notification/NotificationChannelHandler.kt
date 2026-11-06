package com.pulseprice.api.notification

import com.pulseprice.common.event.PriceDropAlertEvent
import com.pulseprice.common.model.NotificationChannel

interface NotificationChannelHandler {
    val supportedChannel: NotificationChannel
    suspend fun sendNotification(event: PriceDropAlertEvent): Boolean
}

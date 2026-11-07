package com.pulseprice.api.service

import com.pulseprice.api.domain.ProductSearchCustomRepository
import com.pulseprice.common.model.PriceHistoryResponse
import kotlinx.coroutines.flow.toList
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Service
class PriceHistoryAnalyticsService(private val searchCustomRepository: ProductSearchCustomRepository) {
    suspend fun getHistory(canonicalProductId: UUID, range: String): PriceHistoryResponse {
        val (since, interval) = when (range.lowercase()) {
            "7d" -> Pair(Instant.now().minus(Duration.ofDays(7)), "1 hour")
            "30d" -> Pair(Instant.now().minus(Duration.ofDays(30)), "1 day")
            "90d" -> Pair(Instant.now().minus(Duration.ofDays(90)), "1 day")
            "1y" -> Pair(Instant.now().minus(Duration.ofDays(365)), "7 days")
            else -> Pair(Instant.now().minus(Duration.ofDays(30)), "1 day")
        }

        val points = searchCustomRepository.findHistoricalPriceBuckets(canonicalProductId, interval, since).toList()
        val bounds = searchCustomRepository.findAllTimeLowAndHigh(canonicalProductId)

        val avg = if (points.isNotEmpty()) {
            val total = points.map { it.avgPrice }.reduce { acc, p -> acc.add(p)}
            total.divide(BigDecimal(points.size), 2, RoundingMode.HALF_UP)
        } else null

        return PriceHistoryResponse(
            canonicalProductId = canonicalProductId,
            timeRange = range,
            currentLowestPrice = points.lastOrNull()?.minPrice,
            allTimeLow = bounds?.first,
            allTimeHigh = bounds?.second,
            averagePrice = avg,
            points = points,
        )
    }
}

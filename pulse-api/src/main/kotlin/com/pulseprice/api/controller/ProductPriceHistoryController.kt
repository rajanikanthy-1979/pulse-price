package com.pulseprice.api.controller

import com.pulseprice.api.service.PriceHistoryAnalyticsService
import com.pulseprice.common.model.PriceHistoryResponse
import org.springframework.web.bind.annotation.*
import java.util.UUID

@RestController
@RequestMapping("/api/v1/products")
class ProductPriceHistoryController(
    private val analyticsService: PriceHistoryAnalyticsService
) {

    @GetMapping("/{id}/history")
    suspend fun getPriceHistory(
        @PathVariable id: UUID,
        @RequestParam(defaultValue = "30d") range: String
    ): PriceHistoryResponse {
        return analyticsService.getHistory(id, range)
    }
}

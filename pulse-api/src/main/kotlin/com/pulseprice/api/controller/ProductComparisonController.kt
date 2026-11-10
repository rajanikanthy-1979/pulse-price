package com.pulseprice.api.controller

import com.pulseprice.api.service.PriceComparisonService
import com.pulseprice.common.model.ProductComparisonResponse
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException
import java.util.UUID

@RestController
@RequestMapping(value = ["/api/v1/products"])
class ProductComparisonController(private val comparisonService: PriceComparisonService) {
    @GetMapping("/{id}/compare")
    suspend fun compareProductPrices(@PathVariable id: UUID) : ProductComparisonResponse {
        return comparisonService.getCrossRetailerComparison(canonicalProductId = id)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Product not found with id: $id")
    }
}

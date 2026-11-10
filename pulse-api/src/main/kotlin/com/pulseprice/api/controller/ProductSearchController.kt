package com.pulseprice.api.controller

import com.pulseprice.api.service.MetasearchProductService
import com.pulseprice.common.model.ProductSearchResult
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping(value = ["/api/v1/products"])
class ProductSearchController(private val searchService: MetasearchProductService) {
    @GetMapping("/search")
    suspend fun searchProducts(
        @RequestParam(defaultValue = "") q: String,
        @RequestParam(required = false) brand: String?,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int
    ): ProductSearchResult {
        val sanitizedPage = page.coerceAtLeast(0)
        val sanitizedSize = size.coerceIn(1, 100)
        return this.searchService.search(q, brand, sanitizedPage, sanitizedSize)
    }

}

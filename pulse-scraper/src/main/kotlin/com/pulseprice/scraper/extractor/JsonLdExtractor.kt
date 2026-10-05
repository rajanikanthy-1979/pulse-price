package com.pulseprice.scraper.extractor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.model.ProductRawPayload
import org.jsoup.nodes.Document
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class JsonLdExtractor(private val objectMapper: ObjectMapper) {
    fun extract(doc: Document): ProductRawPayload? {
        val scripts = doc.select("script[type=application/ld+json]")
        for (script in scripts) {
            val jsonContent = script.data().trim();
            if (jsonContent.isBlank()) continue;
            runCatching {
                val node = objectMapper.readTree(jsonContent)
                val productNode = findProductNode(node)
                if (productNode != null) {
                    return parseProduct(productNode, jsonContent)
                }
            }
        }
        return null
    }

    private fun findProductNode(node: JsonNode): JsonNode? {
        if (node.isArray) {
            return node.firstOrNull { it.path("@type").asText().equals("Product", ignoreCase = true) }
        }
        if (node.path("@type").asText().equals("Product", ignoreCase = true)) {
            return node
        }
        if (node.has("@graph")) {
            return findProductNode(node.path("@graph"))
        }
        return null
    }

    private fun parseProduct(node: JsonNode, rawJson: String): ProductRawPayload {
        val title = node.path("name").asText("")
        val brand = node.path("brand").path("name").asText(null) ?: node.path("brand").asText(null)
        val sku = node.path("sku").asText(null)
        val gtin = node.path("gtin13").asText(null)
            ?: node.path("gtin").asText(null)
            ?: node.path("isbn").asText(null)

        val offers = node.path("offers")
        val offerNode = if (offers.isArray) offers.firstOrNull() ?: offers else offers

        val priceStr = offerNode.path("price").asText("0.00").replace(",", "")
        val price = runCatching { BigDecimal(priceStr) }.getOrDefault(BigDecimal.ZERO)
        val currency = offerNode.path("priceCurrency").asText("USD")
        val availability = offerNode.path("availability").asText("")
        val inStock = availability.contains("InStock", ignoreCase = true)

        return ProductRawPayload(
            title = title,
            currentPrice = price,
            currency = currency,
            sku = sku,
            gtin = gtin,
            brand = brand,
            inStock = inStock,
            rawJsonLd = rawJson
        )
    }
}
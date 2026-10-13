package com.pulseprice.scraper.extractor

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.pulseprice.common.model.ProductRawPayload
import com.pulseprice.common.model.ScrapeTier
import org.jsoup.nodes.Document
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class JsonLdExtractor(private val objectMapper: ObjectMapper): Extractor {

    override fun extract(doc: Document): ProductRawPayload? {
        val scripts = doc.select("script[type=application/ld+json]")
        for (script in scripts) {
            val jsonContent = script.data().trim()
            if (jsonContent.isBlank()) continue

            val result = runCatching {
                val node = objectMapper.readTree(jsonContent)
                val productNode = findProductNode(node)
                if (productNode != null) {
                    parseProduct(productNode, jsonContent)
                } else null
            }.onFailure { ex ->
                log.debug("Failed parsing JSON-LD script block: {}", ex.message)
            }.getOrNull()

            if (result != null) return result
        }
        return null
    }

    private fun findProductNode(node: JsonNode): JsonNode? {
        if (node.isArray) {
            for (element in node) {
                val found = findProductNode(element)
                if (found != null) return found
            }
            return null
        }

        val type = node.path("@type").asText("")
        if (type.equals("Product", ignoreCase = true) || type.equals("IndividualProduct", ignoreCase = true)) {
            return node
        }

        if (node.has("@graph")) {
            return findProductNode(node.path("@graph"))
        }

        if (node.has("mainEntity")) {
            return findProductNode(node.path("mainEntity"))
        }

        return null
    }

    private fun parseProduct(node: JsonNode, rawJson: String): ProductRawPayload? {
        val title = node.path("name").asText("").trim()
        if (title.isBlank()) return null

        val brand = when {
            node.path("brand").isObject -> node.path("brand").path("name").asText(null)
            node.path("brand").isTextual -> node.path("brand").asText(null)
            else -> null
        }

        val sku = node.path("sku").asText(null)
        val gtin = node.path("gtin14").asText(null)
            ?: node.path("gtin13").asText(null)
            ?: node.path("gtin12").asText(null)
            ?: node.path("gtin8").asText(null)
            ?: node.path("gtin").asText(null)
            ?: node.path("isbn").asText(null)

        val offers = node.path("offers")
        val offerNode = when {
            offers.isArray && !offers.isEmpty -> offers[0]
            offers.isObject -> offers
            else -> null
        }

        val (currentPrice, originalPrice, currency, inStock) = if (offerNode != null) {
            val priceStr = offerNode.path("price").asText("")
            val lowPriceStr = offerNode.path("lowPrice").asText("")
            val highPriceStr = offerNode.path("highPrice").asText("")

            val resolvedPrice = when {
                priceStr.isNotBlank() -> parsePrice(priceStr)
                lowPriceStr.isNotBlank() -> parsePrice(lowPriceStr)
                else -> BigDecimal.ZERO
            }

            val origPrice = if (highPriceStr.isNotBlank()) parsePrice(highPriceStr).takeIf { it > resolvedPrice } else null
            val curr = offerNode.path("priceCurrency").asText("USD").cleanCurrency()
            val avail = offerNode.path("availability").asText("")
            val stock = !avail.contains("OutOfStock", ignoreCase = true) && !avail.contains("Discontinued", ignoreCase = true)

            Tuple4(resolvedPrice, origPrice, curr, stock)
        } else {
            Tuple4(BigDecimal.ZERO, null, "USD", true)
        }

        if (currentPrice <= BigDecimal.ZERO) return null

        return ProductRawPayload(
            title = title,
            currentPrice = currentPrice,
            originalPrice = originalPrice,
            sku = sku,
            gtin = gtin,
            brand = brand,
            inStock = inStock,
            currency = currency,
            rawJsonLd = rawJson.take(8192),
            extractionTier = ScrapeTier.FAST_PATH_JSON_LD,
            confidenceScore = if (!gtin.isNullOrBlank()) 1.0 else 0.85
        )
    }

    private fun parsePrice(value: String): BigDecimal {
        val clean = value.replace(Regex("[^0-9.]"), "").trim()
        return runCatching { BigDecimal(clean) }.getOrDefault(BigDecimal.ZERO)
    }

    private fun String.cleanCurrency(): String {
        return when (this.trim().uppercase()) {
            "$", "USD" -> "USD"
            "€", "EUR" -> "EUR"
            "£", "GBP" -> "GBP"
            "CAD" -> "CAD"
            else -> if (this.length == 3) this.uppercase() else "USD"
        }
    }

    private data class Tuple4<A, B, C, D>(val a: A, val b: B, val c: C, val d: D)

    companion object {
        private val log = LoggerFactory.getLogger(JsonLdExtractor::class.java)
    }
}

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
class NextDataExtractor(private val objectMapper: ObjectMapper): Extractor {
    override fun extract(doc: Document): ProductRawPayload? {
        val script = doc.selectFirst("script#__NEXT_DATA__") ?: return null
        val rawJson = script.data().trim()
        if (rawJson.isBlank()) return null

        return runCatching {
            val root = objectMapper.readTree(rawJson)
            val pageProps = root.path("props").path("pageProps")
            if (pageProps.isMissingNode || pageProps.isNull) return null

            findProductPayload(pageProps, rawJson)
        }.onFailure { ex ->
            log.debug("Failed parsing __NEXT_DATA__: {}", ex.message)
        }.getOrNull()
    }

    private fun findProductPayload(pageProps: JsonNode, rawJson: String): ProductRawPayload? {
        val productNode = sequenceOf(
            pageProps.path("product"),
            pageProps.path("productDetails"),
            pageProps.path("initialData").path("product"),
            pageProps.path("data").path("product")
        ).firstOrNull { !it.isMissingNode && !it.isNull } ?: return null

        val title = productNode.path("title").asText(null)
            ?: productNode.path("name").asText(null)
            ?: productNode.path("item").path("product_description").path("title").asText("")

        if (title.isBlank()) return null

        val priceNode = sequenceOf(
            productNode.path("price"),
            productNode.path("currentPrice"),
            productNode.path("pricing").path("currentPrice"),
            productNode.path("price").path("current_retail"),
            productNode.path("offers").path("price")
        ).firstOrNull { !it.isMissingNode && !it.isNull }

        val currentPrice = when {
            priceNode == null -> BigDecimal.ZERO
            priceNode.isNumber -> BigDecimal.valueOf(priceNode.asDouble())
            else -> parsePriceString(priceNode.asText())
        }

        // 3. Extract original / list price
        val originalPriceNode = sequenceOf(
            productNode.path("regularPrice"),
            productNode.path("listPrice"),
            productNode.path("pricing").path("regPrice"),
            productNode.path("price").path("reg_retail")
        ).firstOrNull { !it.isMissingNode && !it.isNull }

        val originalPrice = when {
            originalPriceNode == null -> null
            originalPriceNode.isNumber -> BigDecimal.valueOf(originalPriceNode.asDouble())
            else -> parsePriceString(originalPriceNode.asText()).takeIf { it > currentPrice }
        }

        // 4. Extract GTIN, SKU, Brand
        val gtin = productNode.path("gtin").asText(null)
            ?: productNode.path("upc").asText(null)
            ?: productNode.path("barcode").asText(null)

        val sku = productNode.path("sku").asText(null)
            ?: productNode.path("tcin").asText(null)
            ?: productNode.path("itemId").asText(null)

        val brand = productNode.path("brand").asText(null)
            ?: productNode.path("brand").path("name").asText(null)

        val currency = productNode.path("currencyCode").asText("USD")
            .ifBlank { "USD" }

        val inStock = productNode.path("inStock").asBoolean(true)

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
            extractionTier = ScrapeTier.FAST_PATH_NEXT_DATA,
            confidenceScore = calculateConfidence(title, currentPrice, gtin)
        )
    }

    private fun parsePriceString(value: String): BigDecimal {
        val clean = value.replace(Regex("[^0-9.]"), "").trim()
        return runCatching { BigDecimal(clean) }.getOrDefault(BigDecimal.ZERO)
    }

    private fun calculateConfidence(title: String, price: BigDecimal, gtin: String?): Double {
        var score = 0.5
        if (title.length > 5) score += 0.2
        if (price > BigDecimal.ZERO) score += 0.2
        if (!gtin.isNullOrBlank()) score += 0.1
        return score.coerceAtMost(1.0)
    }

    companion object {
        private val log = LoggerFactory.getLogger(NextDataExtractor::class.java)
    }
}

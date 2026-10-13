package com.pulseprice.scraper.extractor

import com.pulseprice.common.model.ProductRawPayload
import com.pulseprice.common.model.ScrapeTier
import org.jsoup.nodes.Document
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class OpenGraphFallbackExtractor: Extractor {

    override fun extract(doc: Document): ProductRawPayload? {
        val title = doc.selectFirst("meta[property=og:title]")?.attr("content")
            ?: doc.selectFirst("meta[name=twitter:title]")?.attr("content")
            ?: doc.title()

        if (title.isBlank()) return null

        val priceMeta = doc.selectFirst("meta[property=og:price:amount]")?.attr("content")
            ?: doc.selectFirst("meta[property=product:price:amount]")?.attr("content")
            ?: doc.selectFirst("meta[itemprop=price]")?.attr("content")
            ?: doc.selectFirst("[itemprop=price]")?.text()

        if (priceMeta.isNullOrBlank()) return null

        val cleanPrice = priceMeta.replace(Regex("[^0-9.]"), "").trim()
        val price = runCatching { BigDecimal(cleanPrice) }.getOrDefault(BigDecimal.ZERO)
        if (price <= BigDecimal.ZERO) return null

        val currency = doc.selectFirst("meta[property=og:price:currency]")?.attr("content")
            ?: doc.selectFirst("meta[property=product:price:currency]")?.attr("content")
            ?: doc.selectFirst("meta[itemprop=priceCurrency]")?.attr("content")
            ?: "USD"

        val imageUrl = doc.selectFirst("meta[property=og:image]")?.attr("content")

        return ProductRawPayload(
            title = title.trim(),
            currentPrice = price,
            currency = currency.trim().uppercase(),
            imageUrl = imageUrl,
            inStock = true,
            extractionTier = ScrapeTier.FAST_PATH_OPEN_GRAPH,
            confidenceScore = 0.65
        )
    }
}

package com.pulseprice.scraper.extractor

import com.pulseprice.common.model.ProductRawPayload
import org.jsoup.nodes.Document
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class FastPathCompositeExtractor(
    private val nextDataExtractor: NextDataExtractor,
    private val jsonLdExtractor: JsonLdExtractor,
    private val openGraphFallbackExtractor: OpenGraphFallbackExtractor
) {

    fun extract(doc: Document): ProductRawPayload? {
        // Priority 1: Next.js hydration payload (highest fidelity, zero CSS drift)
        nextDataExtractor.extract(doc)?.let {
            log.debug("Extracted product via NextDataExtractor: {}", it.title)
            return it
        }

        // Priority 2: Schema.org JSON-LD (standard semantic markup)
        jsonLdExtractor.extract(doc)?.let {
            log.debug("Extracted product via JsonLdExtractor: {}", it.title)
            return it
        }

        // Priority 3: OpenGraph & Microdata meta tags
        openGraphFallbackExtractor.extract(doc)?.let {
            log.debug("Extracted product via OpenGraphFallbackExtractor: {}", it.title)
            return it
        }

        return null
    }

    companion object {
        private val log = LoggerFactory.getLogger(FastPathCompositeExtractor::class.java)
    }
}

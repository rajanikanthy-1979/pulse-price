package com.pulseprice.scraper.extractor

import com.pulseprice.common.model.ProductRawPayload
import org.jsoup.nodes.Document

interface Extractor {
    fun extract(doc: Document): ProductRawPayload?
}

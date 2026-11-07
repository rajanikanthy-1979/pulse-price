package com.pulseprice.api.service

import com.pulseprice.api.domain.CanonicalProductRepository
import com.pulseprice.api.domain.ProductSearchCustomRepository
import com.pulseprice.common.model.ProductComparisonResponse
import com.pulseprice.common.model.RetailOfferDto
import kotlinx.coroutines.flow.toList
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.util.UUID

@Service
class PriceComparisonService(
    private val canonicalProductRepository: CanonicalProductRepository,
    private val searchCustomRepository: ProductSearchCustomRepository
) {
    suspend fun getCrossRetailerComparison(canonicalProductId: UUID) : ProductComparisonResponse? {
        val canonical = canonicalProductRepository.findById(canonicalProductId) ?:  return null
        val offers = searchCustomRepository.findOffersForProduct(canonicalProductId).toList()

        if (offers.isEmpty()) {
            return ProductComparisonResponse(
                canonicalProductId = canonical.id!!,
                gtin = canonical.gtin,
                brand = canonical.brand,
                model = canonical.model,
                title = canonical.title,
                lowestPrice = BigDecimal.ZERO,
                highestPrice = BigDecimal.ZERO,
                priceSpread = BigDecimal.ZERO,
                bestRetailer = "None",
                currency = "USD",
                offers = emptyList()
            )
        }

        val inStockOffers = offers.filter { it.inStock }
        val targetOffers = inStockOffers.ifEmpty { offers }
        val lowestOffer = targetOffers.minByOrNull { it.price } ?: offers.first()
        val highestOffer = targetOffers.maxByOrNull { it.price } ?: offers.first()

        val offerDtos = offers.map { offer ->
            val diff = offer.price.subtract(lowestOffer.price)
            RetailOfferDto(
                retailerProductId = offer.retailerProductId,
                retailer = offer.retailer,
                retailerSku = offer.retailerSku,
                url = offer.url,
                title = offer.title,
                currency = offer.currency,
                currentPrice = offer.price,
                originalPrice = offer.originalPrice,
                inStock = offer.inStock,
                isBestPrice = (offer.retailerProductId == lowestOffer.retailerProductId),
                priceDifferenceFromLowest = diff,
                lastUpdated = offer.recordedAt
            )
        }.sortedWith(compareBy( {!it.inStock}, {it.currentPrice}))

        return ProductComparisonResponse(
            canonicalProductId = canonical.id!!,
            gtin = canonical.gtin,
            brand = canonical.brand,
            model = canonical.model,
            title = canonical.title,
            lowestPrice = lowestOffer.price,
            highestPrice = highestOffer.price,
            priceSpread = highestOffer.price.subtract(lowestOffer.price),
            bestRetailer = lowestOffer.retailer,
            currency = lowestOffer.currency,
            offers = offerDtos
        )
    }
}

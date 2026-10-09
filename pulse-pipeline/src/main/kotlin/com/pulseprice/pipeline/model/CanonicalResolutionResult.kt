package com.pulseprice.pipeline.model

import com.pulseprice.pipeline.domain.CanonicalProductEntity
import com.pulseprice.pipeline.domain.RetailerProductEntity

data class CanonicalResolutionResult(
    val canonicalProduct: CanonicalProductEntity,
    val retailerProduct: RetailerProductEntity
)

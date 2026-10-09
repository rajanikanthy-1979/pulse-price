package com.pulseprice.pipeline.resolver

import org.springframework.stereotype.Component

@Component
class GtinValidator {
    /**
     * Validates and normalizes GTIN-8, GTIN-12 (UPC-A), GTIN-13 (EAN), and GTN-14
     * Returns a 14-digit zero padded canonical GTIN String, or null if invalid
     */
    fun validateAndNormalize(rawGtin: String?): String? {
        if (rawGtin.isNullOrBlank()) return null
        val digitsOnly = rawGtin.filter { it.isDigit() }
        if (digitsOnly.length !in setOf(8, 12, 13,14)) return null
        if (!hasValidModulo10Checksum(digitsOnly)) return null
        return digitsOnly.padStart(14, '0')
    }

    /**
     * GS1 standard Modulo-10 check digit verification
     */
    private fun hasValidModulo10Checksum(code: String): Boolean {
        val length = code.length
        val expectedCheckDigit = code.last().digitToInt()
        val dataPayload = code.substring(0, length - 1)

        var sum = 0;
        var weight = 3
        for(i in dataPayload.length - 1 downTo 0) {
            sum += dataPayload[i].digitToInt() * weight
            weight = if (weight == 3) 1 else 3
        }
        val calculatedCheckDigit = (10 - (sum % 10)) % 10
        return calculatedCheckDigit == expectedCheckDigit
    }
}

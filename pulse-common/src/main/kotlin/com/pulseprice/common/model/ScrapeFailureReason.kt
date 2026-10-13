package com.pulseprice.common.model

enum class ScrapeFailureReason {
    ANTI_BOT_CHALLENGE,
    CAPTCHA_DETECTED,
    DOM_DRIFT,
    EMPTY_PAYLOAD,
    HTTP_STATUS_ERROR,
    RATE_LIMITED,
    TIMEOUT,
    PARSING_ERROR
}

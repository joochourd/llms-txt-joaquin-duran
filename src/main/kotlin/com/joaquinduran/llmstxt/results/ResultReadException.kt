package com.joaquinduran.llmstxt.results

import com.joaquinduran.llmstxt.ingestion.DomainError

class ResultReadException(
    val error: DomainError,
) : RuntimeException(error.message)

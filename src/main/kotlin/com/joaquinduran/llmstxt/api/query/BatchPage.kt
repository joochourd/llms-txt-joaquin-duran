package com.joaquinduran.llmstxt.api.query

import com.joaquinduran.llmstxt.api.errors.ApiProblem

data class BatchPage(
    val offset: Int = 0,
    val limit: Int = MAX_PAGE_SIZE,
) {
    init {
        if (offset < 0 || limit !in 1..MAX_PAGE_SIZE) {
            throw ApiProblem(400, "INVALID_PAGE", "Use a nonnegative offset and a limit between 1 and $MAX_PAGE_SIZE.")
        }
    }

    fun end(total: Int): Int = minOf(total.toLong(), offset.toLong() + limit).toInt()

    fun nextOffset(total: Int): Int? = end(total).takeIf { it < total }

    companion object {
        const val MAX_PAGE_SIZE = 100
    }
}

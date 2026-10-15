package com.joaquinduran.llmstxt.results

data class BatchArtifact(
    val seedUrl: String,
    val sequence: Long,
    val hash: String,
    val bytes: ByteArray,
)

package com.joaquinduran.llmstxt.results

sealed interface ArtifactRead {
    val outputHash: String
    val normalizedSeedUrl: String
    val versionSequence: Long?

    data class Downloaded(
        override val outputHash: String,
        override val normalizedSeedUrl: String,
        override val versionSequence: Long?,
        val bytes: ByteArray,
    ) : ArtifactRead

    data class Cached(
        override val outputHash: String,
        override val normalizedSeedUrl: String,
        override val versionSequence: Long?,
    ) : ArtifactRead
}

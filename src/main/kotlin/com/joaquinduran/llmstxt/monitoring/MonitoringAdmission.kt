package com.joaquinduran.llmstxt.monitoring

enum class MonitoringAdmissionKind { ENQUEUED, COALESCED, DEFERRED, SKIPPED }

data class MonitoringAdmission(
    val kind: MonitoringAdmissionKind,
    val runId: String? = null,
)

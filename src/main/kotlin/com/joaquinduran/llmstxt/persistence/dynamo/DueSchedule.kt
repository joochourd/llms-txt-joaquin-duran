package com.joaquinduran.llmstxt.persistence.dynamo

import com.joaquinduran.llmstxt.ingestion.CrawlMeta
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

/** Every transition derives the sparse hint from the resulting base-table state. */
internal object DueSchedule {
    fun append(
        meta: CrawlMeta,
        shards: Int,
        set: MutableList<String>,
        remove: MutableList<String>,
        values: MutableMap<String, AttributeValue>,
        state: MonitoringState = meta.monitoringState,
        nextCheckAt: Long? = meta.nextCheckAt,
        expiresAt: Long? = meta.monitoringExpiresAt,
        reservationExpiresAt: Long? = meta.monitoringReservationExpiresAt,
    ) {
        val action = when (state) {
            MonitoringState.ACTIVE -> listOfNotNull(nextCheckAt, expiresAt).minOrNull()
            MonitoringState.RESERVED -> reservationExpiresAt
            else -> null
        }
        if (action == null) {
            remove.addAll(listOf("duePk", "dueSk", "dueShard"))
            return
        }
        val shard = DomainKeys.shard(meta.identityId, shards)
        set.addAll(listOf("dueShard = :dueShard", "duePk = :duePk", "dueSk = :dueSk"))
        values[":dueShard"] = number(shard.toLong())
        values[":duePk"] = text("DUE#" + shard)
        values[":dueSk"] = text(DomainKeys.ordered(action) + "#" + meta.identityId)
    }
}

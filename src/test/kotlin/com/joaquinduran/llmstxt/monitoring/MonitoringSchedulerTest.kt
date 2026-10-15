package com.joaquinduran.llmstxt.monitoring

import com.joaquinduran.llmstxt.ingestion.*
import com.joaquinduran.llmstxt.ingestion.Capacity
import com.joaquinduran.llmstxt.ingestion.MonitoringState
import com.joaquinduran.llmstxt.ingestion.UrlIdentityResolver
import com.joaquinduran.llmstxt.persistence.dynamo.*
import com.joaquinduran.llmstxt.persistence.dynamo.DomainKeys
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class MonitoringSchedulerTest : MonitoringFixture() {
    @Test
    fun `due shard rotation and continuation reach backlog beyond the per cycle limit`() {
        config.monitoring.schedulerCandidateLimit = 2
        val urls = UrlIdentityResolver(settings)
        val seeds = (1..100)
            .map { "https://site$it.example/" }
            .filter {
                DomainKeys.shard(urls.normalize(it).identityId, store.configuration.dueIndexShards) == 0
            }.take(9)
        val metas = seeds.map(::canonical)
        clock.now = metas.maxOf { requireNotNull(it.nextCheckAt) }
        val scheduler = scheduler()
        try {
            await({ metas.all { store.meta(it.identityId)?.lastMonitoringRunId != null } }) {
                assertTrue(scheduler.tick() <= config.monitoring.schedulerCandidateLimit)
            }
            assertEquals(Capacity(9, 9), store.capacity())
            metas.forEach { assertEquals(1L, store.meta(it.identityId)?.outstandingRunCount) }
        } finally {
            scheduler.close()
        }
    }

    @Test
    fun `stale due hints never authorize disabled identities and active expiry needs no browser`() {
        val first = canonical("https://first.example/")
        val second = canonical("https://second.example/")
        monitoring.change(first.identityId, false)
        val shard = DomainKeys.shard(first.identityId, store.configuration.dueIndexShards)
        update(
            DomainKeys.crawl(first.identityId),
            "SET duePk = :pk, dueSk = :sk",
            mapOf(":pk" to text("DUE#$shard"), ":sk" to text(DomainKeys.ordered(clock.now) + "#" + first.identityId)),
        )
        clock.now = requireNotNull(second.monitoringExpiresAt)
        val scheduler = scheduler()
        try {
            await({ store.meta(second.identityId)?.monitoringState == MonitoringState.EXPIRED }) { scheduler.tick() }
            assertNull(store.meta(first.identityId)?.lastMonitoringRunId)
            assertEquals(Capacity(0, 0), store.capacity())
            assertEquals(second.currentArtifactHash, store.meta(second.identityId)?.currentArtifactHash)
        } finally {
            scheduler.close()
        }
    }

    @Test
    fun `legacy cleanup hints are removed or repaired without renewing monitoring or changing counters`() {
        val inactive = canonical("https://inactive.example/")
        val active = canonical("https://active.example/")
        monitoring.change(inactive.identityId, false)
        for (meta in listOf(inactive, active)) {
            val shard = DomainKeys.shard(meta.identityId, store.configuration.dueIndexShards)
            update(
                DomainKeys.crawl(meta.identityId),
                "SET cleanupAt = :now, duePk = :pk, dueSk = :sk, dueShard = :shard",
                mapOf(
                    ":now" to number(clock.now),
                    ":pk" to text("DUE#$shard"),
                    ":sk" to text(DomainKeys.ordered(clock.now) + "#" + meta.identityId),
                    ":shard" to number(shard.toLong()),
                ),
            )
        }
        val scheduler = scheduler()

        try {
            await({
                store.meta(inactive.identityId)?.duePk == null &&
                    store.meta(active.identityId)?.dueSk == DomainKeys.ordered(requireNotNull(active.nextCheckAt)) + "#" + active.identityId
            }) { scheduler.tick() }

            assertEquals(active.nextCheckAt, store.meta(active.identityId)?.nextCheckAt)
            assertEquals(active.monitoringExpiresAt, store.meta(active.identityId)?.monitoringExpiresAt)
            assertNull(store.meta(active.identityId)?.lastMonitoringRunId)
            assertEquals(Capacity(0, 1), store.capacity())
        } finally {
            scheduler.close()
        }
    }
}

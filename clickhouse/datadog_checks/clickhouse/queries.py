# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from .utils import compact_query


class Query(object):
    pass


class SystemMetrics(Query):
    """
    https://clickhouse.yandex/docs/en/operations/system_tables/#system_tables-metrics
    """

    fields = (
        ('BackgroundPoolTask', (('background_pool.processing.task.active', 'gauge'),)),
        ('BackgroundSchedulePoolTask', (('background_pool.schedule.task.active', 'gauge'),)),
        ('ContextLockWait', (('thread.lock.context.waiting', 'gauge'),)),
        ('DelayedInserts', (('ddl.insert.delayed', 'gauge'),)),
        ('DictCacheRequests', (('dictionary.request.cache', 'gauge'),)),
        ('DiskSpaceReservedForMerge', (('merge.disk.reserved', 'gauge'),)),
        ('DistributedFilesToInsert', (('table.distributed.file.insert.pending', 'gauge'),)),
        ('DistributedSend', (('table.distributed.connection.inserted', 'gauge'),)),
        ('EphemeralNode', (('zk.node.ephemeral', 'gauge'),)),
        ('GlobalThread', (('thread.global.total', 'gauge'),)),
        ('GlobalThreadActive', (('thread.global.active', 'gauge'),)),
        ('HTTPConnection', (('connection.http', 'gauge'),)),
        ('InterserverConnection', (('connection.interserver', 'gauge'),)),
        ('LeaderElection', (('replica.leader.election', 'gauge'),)),
        ('LeaderReplica', (('table.replicated.leader', 'gauge'),)),
        ('LocalThread', (('thread.local.total', 'gauge'),)),
        ('LocalThreadActive', (('thread.local.active', 'gauge'),)),
        ('MemoryTracking', (('query.memory', 'gauge'),)),
        ('MemoryTrackingForMerges', (('merge.memory', 'gauge'),)),
        ('MemoryTrackingInBackgroundProcessingPool', (('background_pool.processing.memory', 'gauge'),)),
        ('MemoryTrackingInBackgroundSchedulePool', (('background_pool.schedule.memory', 'gauge'),)),
        ('Merge', (('merge.active', 'gauge'),)),
        ('OpenFileForRead', (('file.open.read', 'gauge'),)),
        ('OpenFileForWrite', (('file.open.write', 'gauge'),)),
        ('PartMutation', (('ddl.mutation', 'gauge'),)),
        ('Query', (('query.active', 'gauge'),)),
        ('QueryPreempted', (('query.waiting', 'gauge'),)),
        ('QueryThread', (('thread.query', 'gauge'),)),
        ('RWLockActiveReaders', (('thread.lock.rw.active.read', 'gauge'),)),
        ('RWLockActiveWriters', (('thread.lock.rw.active.write', 'gauge'),)),
        ('RWLockWaitingReaders', (('thread.lock.rw.waiting.read', 'gauge'),)),
        ('RWLockWaitingWriters', (('thread.lock.rw.waiting.write', 'gauge'),)),
        ('Read', (('syscall.read', 'gauge'),)),
        ('ReadonlyReplica', (('table.replicated.readonly', 'gauge'),)),
        ('ReplicatedChecks', (('replication_data.check', 'gauge'),)),
        ('ReplicatedFetch', (('replication_data.fetch', 'gauge'),)),
        ('ReplicatedSend', (('replication_data.send', 'gauge'),)),
        ('SendExternalTables', (('connection.send.external', 'gauge'),)),
        ('StorageBufferBytes', (('table.buffer.size', 'gauge'),)),
        ('StorageBufferRows', (('table.buffer.row', 'gauge'),)),
        ('TCPConnection', (('connection.tcp', 'gauge'),)),
        ('Write', (('syscall.write', 'gauge'),)),
        ('ZooKeeperRequest', (('zk.request', 'gauge'),)),
        ('ZooKeeperSession', (('zk.connection', 'gauge'),)),
        ('ZooKeeperWatch', (('zk.watch', 'gauge'),)),
    )
    views = ('system.metrics',)
    query = compact_query(
        """
        SELECT
          value
        FROM {view1}
        WHERE metric in ({columns})
        ORDER BY metric
        """.format(
            columns=', '.join("'{}'".format(field[0]) for field in fields),
            **{'view{}'.format(i): view for i, view in enumerate(views, 1)}
        )
    )


class SystemEvents(Query):
    """
    https://clickhouse.yandex/docs/en/operations/system_tables/#system_tables-events
    """

    fields = (
        ('ArenaAllocBytes', (('ArenaAllocBytes', 'gauge'),)),
        ('ArenaAllocChunks', (('ArenaAllocChunks', 'gauge'),)),
        ('CompressedReadBufferBlocks', (('CompressedReadBufferBlocks', 'gauge'),)),
        ('CompressedReadBufferBytes', (('CompressedReadBufferBytes', 'gauge'),)),
        ('ContextLock', (
            ('lock.context.acquisition.count', 'monotonic_count'), ('lock.context.acquisition.total', 'gauge')
        )),
        ('DiskWriteElapsedMicroseconds', ()),
        ('FileOpen', (('file.open.count', 'monotonic_count'), ('file.open.total', 'gauge'))),
        ('FunctionExecute', (('FunctionExecute', 'gauge'),)),
        ('HardPageFaults', (('HardPageFaults', 'gauge'),)),
        ('IOBufferAllocBytes', (('IOBufferAllocBytes', 'gauge'),)),
        ('IOBufferAllocs', (('IOBufferAllocs', 'gauge'),)),
        ('NetworkReceiveElapsedMicroseconds', (('NetworkReceiveElapsedMicroseconds', 'gauge'),)),
        ('NetworkSendElapsedMicroseconds', (('NetworkSendElapsedMicroseconds', 'gauge'),)),
        ('Query', (('query.count', 'monotonic_count'), ('query.total', 'gauge'))),
        ('RWLockAcquiredReadLocks', (('RWLockAcquiredReadLocks', 'gauge'),)),
        ('ReadBufferFromFileDescriptorRead', (('file.read.count', 'monotonic_count'), ('file.read.total', 'gauge'))),
        ('ReadCompressedBytes', (('ReadCompressedBytes', 'gauge'),)),
        ('RealTimeMicroseconds', ()),
        ('SelectQuery', (('query.select.count', 'monotonic_count'), ('query.select.total', 'gauge'))),
        ('SoftPageFaults', (('SoftPageFaults', 'gauge'),)),
        ('SystemTimeMicroseconds', ()),
        ('UserTimeMicroseconds', ()),
        ('WriteBufferFromFileDescriptorWrite', (
            ('file.write.count', 'monotonic_count'), ('file.write.total', 'gauge')
        )),
        ('WriteBufferFromFileDescriptorWriteBytes', (
            ('file.write.size.count', 'monotonic_count'), ('file.write.size.total', 'gauge')
        )),
    )
    views = ('system.events',)
    query = compact_query(
        """
        SELECT
          value
        FROM {view1}
        WHERE event in ({columns})
        ORDER BY event
        """.format(
            columns=', '.join("'{}'".format(field[0]) for field in fields),
            **{'view{}'.format(i): view for i, view in enumerate(views, 1)}
        )
    )

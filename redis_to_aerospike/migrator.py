"""The multi-threaded producer/consumer migration pipeline.

A single producer (the calling thread) streams records from the source onto a
bounded queue. ``workers`` consumer threads pull records, convert them, run any
transforms, and write to the sink. The bounded queue provides back-pressure so a
fast Redis SCAN never runs the process out of memory waiting on Aerospike writes.

When ``write_batch_size > 1`` each worker buffers records and flushes them with a
single ``batch_write``. Buffering is per-worker (thread-local, no shared lock), so
up to ``workers * write_batch_size`` records may be in flight in addition to the
queue, and every worker flushes its own trailing partial batch on shutdown.

Each record is processed in isolation: a conversion or write failure is recorded
in :class:`MigrationStats` and the pipeline keeps going.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import Iterator, List, Optional, Protocol

from .aerospike_sink import BATCH_RECORD_EXISTS_OUTCOME, RecordAlreadyExists
from .config import HashStrategy, MigrationConfig, RecordExistsPolicy
from .converters.registry import ConverterRegistry, UnsupportedTypeError
from .models import AerospikeRecord, RedisRecord
from .progress import ProgressReporter
from .ratelimit import TokenBucket
from .set_routing import SetRouter
from .stats import MigrationStats
from .transforms import Transform, apply_all

logger = logging.getLogger(__name__)

# Sentinel placed on the queue to tell a worker to shut down.
_STOP = object()


class _Source(Protocol):
    def iter_records(self, batch_size: int) -> Iterator[RedisRecord]: ...


class _Sink(Protocol):
    def write(self, record: AerospikeRecord) -> None: ...

    # Used only in batch mode (write_batch_size > 1).
    def write_many(self, records: List[AerospikeRecord]) -> List[Optional[str]]: ...


class _RateLimiter(Protocol):
    def acquire(self, tokens: float = ...) -> None: ...


class Migrator:
    def __init__(
        self,
        config: MigrationConfig,
        source: _Source,
        registry: ConverterRegistry,
        sink: _Sink,
        transforms: Optional[List[Transform]] = None,
        stats: Optional[MigrationStats] = None,
    ):
        self._config = config
        self._source = source
        self._registry = registry
        self._sink = sink
        self._transforms = transforms or []
        self.stats = stats or MigrationStats()
        self._queue: "queue.Queue" = queue.Queue(maxsize=config.queue_size)
        self._batch_size = max(1, config.write_batch_size)
        self._set_router = SetRouter(
            list(config.aerospike.set_routes),
            config.aerospike.set_name,
        )
        # Optional throttles (disabled when the configured rate is 0). The write
        # limiter is shared by every worker thread, so it must be thread-safe.
        # Its burst is widened to fit a whole batch so flushing one batch is not
        # fragmented across several waits.
        self._scan_limiter: _RateLimiter = TokenBucket(config.scan_rate_limit)
        self._write_limiter: _RateLimiter = TokenBucket(
            config.write_rate_limit,
            capacity=max(config.write_rate_limit, self._batch_size),
        )

    def write_redis_record(self, record: RedisRecord, *, full_replace: bool = False) -> bool:
        """Convert and write one Redis key. False means the key was skipped.

        Pub/sub passes ``full_replace`` so the Aerospike record matches the
        value just read from Redis, including hash fields that were removed.
        """
        prepared = self._prepare(record)
        if prepared is None:
            return False
        prepared.full_replace = full_replace
        self._write_one(prepared, record.key)
        return True

    def delete_key(self, key: str | bytes) -> None:
        """Remove the Aerospike record that ``key`` maps to, including set routes."""
        remove = getattr(self._sink, "remove", None)
        if remove is None:
            raise RuntimeError("sink does not support remove")
        resolution = self._set_router.resolve(key)
        try:
            self._write_limiter.acquire(1)
            remove(resolution.key, resolution.set_name)
        except Exception as exc:
            self.stats.record_error(f"delete:{type(exc).__name__}")
            logger.warning("delete failed for key '%s': %s", key, exc)
            return
        self.stats.record_deleted()

    def run(self) -> MigrationStats:
        self.stats.start()
        progress = ProgressReporter(self.stats, self._config.progress_interval).start()
        workers = self._start_workers()
        try:
            self._produce()
        finally:
            # Signal every worker to stop, then wait for the queue to drain.
            for _ in workers:
                self._queue.put(_STOP)
            for worker in workers:
                worker.join()
            self.stats.finish()
            progress.stop()
        logger.info("migration complete: %s", self.stats.summary())
        return self.stats

    def _start_workers(self) -> List[threading.Thread]:
        workers = []
        for i in range(max(1, self._config.workers)):
            worker = threading.Thread(target=self._worker_loop, name=f"migrator-worker-{i}", daemon=True)
            worker.start()
            workers.append(worker)
        return workers

    def _produce(self) -> None:
        for record in self._source.iter_records(self._config.scan_batch):
            self._scan_limiter.acquire()
            self.stats.record_scanned()
            self._queue.put(record)

    def _worker_loop(self) -> None:
        # In single mode (buffer is None) each record is written immediately. In
        # batch mode records accumulate until the buffer fills; the outer finally
        # flushes whatever remains when the worker is told to stop.
        buffer = None if self._batch_size <= 1 else []
        try:
            while True:
                item = self._queue.get()
                try:
                    if item is _STOP:
                        return
                    prepared = self._prepare(item)
                    if prepared is None:
                        continue
                    if buffer is None:
                        self._write_one(prepared, item.key)
                    else:
                        buffer.append((prepared, item.key))
                        if len(buffer) >= self._batch_size:
                            self._flush(buffer)
                            buffer = []
                finally:
                    self._queue.task_done()
        finally:
            if buffer:
                self._flush(buffer)

    def _prepare(self, record: RedisRecord) -> Optional[AerospikeRecord]:
        """Convert and transform a record, counting skips/conversion errors."""
        resolution = self._set_router.resolve(record.key)
        effective_hash = (
            resolution.hash_strategy
            if resolution.hash_strategy is not None
            else self._config.hash_strategy
        )
        if effective_hash is HashStrategy.MAP_BIN:
            effective_value_bin = (
                resolution.value_bin
                if resolution.value_bin is not None
                else self._config.aerospike.value_bin
            )
        else:
            effective_value_bin = None

        try:
            if record.type == "hash":
                aerospike_record = self._registry.convert(
                    record,
                    hash_strategy=effective_hash,
                    value_bin=effective_value_bin,
                )
            else:
                aerospike_record = self._registry.convert(record)
        except UnsupportedTypeError:
            self.stats.record_skipped(record.type)
            logger.debug("skipping unsupported type '%s' for key '%s'", record.type, record.key)
            return None
        except Exception as exc:  # conversion error
            self.stats.record_error(f"convert:{type(exc).__name__}")
            logger.warning("conversion failed for key '%s': %s", record.key, exc)
            return None

        aerospike_record = apply_all(aerospike_record, self._transforms)
        if resolution.set_name != self._config.aerospike.set_name:
            aerospike_record.set_name = resolution.set_name
        if resolution.key != record.key:
            aerospike_record.key = resolution.key
        return aerospike_record

    def _write_one(self, record: AerospikeRecord, key) -> None:
        """Single-write path: throttle by one record, then write."""
        try:
            self._write_limiter.acquire(1)
            self._sink.write(record)
        except RecordAlreadyExists:
            self.stats.record_skipped("exists")
            logger.debug("skipped existing key '%s' (create_only)", key)
            return
        except Exception as exc:  # write error
            self.stats.record_error(f"write:{type(exc).__name__}")
            logger.warning("write failed for key '%s': %s", key, exc)
            return

        self.stats.record_migrated()

    def _flush(self, buffer: List) -> None:
        """Batch-write path: throttle by record count, then write the batch.

        Never raises: a whole-batch failure (an exception from ``write_many``)
        counts every record as a write error, while per-record failures reported
        in the result list are counted individually so the rest still migrate.
        """
        records = [record for record, _ in buffer]
        self._write_limiter.acquire(len(records))
        try:
            results = self._sink.write_many(records)
        except Exception as exc:  # whole-batch failure
            reason = f"write:{type(exc).__name__}"
            for _, key in buffer:
                self.stats.record_error(reason)
                logger.warning("batch write failed for key '%s': %s", key, exc)
            return

        for (_, key), outcome in zip(buffer, results):
            if outcome is None:
                self.stats.record_migrated()
            elif (
                self._config.aerospike.record_exists_policy is RecordExistsPolicy.CREATE_ONLY
                and outcome == BATCH_RECORD_EXISTS_OUTCOME
            ):
                self.stats.record_skipped("exists")
                logger.debug("skipped existing key '%s' (create_only batch)", key)
            else:
                self.stats.record_error(f"write:{outcome}")
                logger.warning("write failed for key '%s': %s", key, outcome)

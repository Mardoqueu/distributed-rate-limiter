package com.ratelimiter.limiter;

import com.ratelimiter.config.RateLimiterConfig;
import com.ratelimiter.store.DistributedKeyValueStore;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A high-throughput distributed rate limiter designed for fleet-wide enforcement.
 *
 * <h3>Strategy: Local Batching + Sharded Keys</h3>
 * <p>
 * At 100 million requests/minute, calling the distributed store on every request would
 * overwhelm it with ~1.67M writes/sec to a single key. This class addresses the problem
 * with two complementary techniques:
 * <ol>
 *   <li><b>Local batching</b> — requests are counted in-memory and flushed to the store
 *       in batches when a configurable threshold is reached, reducing network calls by
 *       orders of magnitude (e.g., batch size 100 → ~16K store calls/sec total).</li>
 *   <li><b>Key sharding</b> — each flush writes to one of N shard keys
 *       ({@code key:window:shard}), distributing write load across multiple keys in the
 *       store and eliminating hot-key pressure on any single partition.</li>
 * </ol>
 *
 * <h3>Accuracy Model</h3>
 * <p>
 * The rate limit decision is based on an <b>optimistic local estimate</b>:
 * {@code estimatedGlobal = sum(lastKnownShardCounts) + unflushedLocalCount}. Between
 * flushes, each server is unaware of counts from other servers, so the system may
 * temporarily allow slightly more than the configured limit (bounded by
 * {@code numServers × effectiveBatchSize}). This satisfies the "at least" guarantee:
 * clients can always make at least {@code limit} requests, and may briefly exceed it.
 *
 * <h3>Time Windowing</h3>
 * <p>
 * Uses fixed 60-second windows aligned to clock minutes. Each window generates unique
 * store keys that auto-expire via TTL. A known trade-off of fixed windows is the
 * boundary-burst issue (a burst straddling two windows can allow up to 2× the limit
 * in a short span); a sliding-window or dual-counter approach would mitigate this at
 * the cost of additional complexity and store calls.
 *
 * <h3>Thread Safety</h3>
 * <p>
 * All mutable state is managed via {@link ConcurrentHashMap}, {@link AtomicInteger},
 * and {@link AtomicBoolean} — no locks are held on the hot path.
 */
public class DistributedHighThroughputRateLimiter implements AutoCloseable {

    /**
     * Extra TTL added to keys so they survive the full window duration even when the
     * first flush happens partway through the window.
     */
    private static final int EXPIRATION_BUFFER_SECONDS = 5;

    /**
     * The effective batch size is capped at {@code limit / OVERSHOOT_DIVISOR}, ensuring
     * each server's unflushed count stays within a bounded fraction of the configured limit.
     */
    private static final int OVERSHOOT_DIVISOR = 10;

    private final DistributedKeyValueStore store;
    private final RateLimiterConfig config;
    private final Clock clock;
    private final ConcurrentHashMap<String, WindowState> windows = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public DistributedHighThroughputRateLimiter(DistributedKeyValueStore store, RateLimiterConfig config) {
        this(store, config, Clock.systemUTC());
    }

    /**
     * Package-private constructor allowing clock injection for deterministic testing.
     */
    DistributedHighThroughputRateLimiter(DistributedKeyValueStore store,
                                         RateLimiterConfig config,
                                         Clock clock) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rate-limiter-sync");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(
                this::periodicSync,
                config.syncIntervalMs(),
                config.syncIntervalMs(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Determines whether a request identified by the given key should be allowed
     * under the configured rate limit.
     *
     * @param key   the client or resource identifier (e.g., a client ID)
     * @param limit the maximum number of requests allowed within the 60-second window
     * @return a future resolving to {@code true} if the request is within the limit
     */
    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        Objects.requireNonNull(key, "key must not be null");
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }

        long windowId = currentWindowId();
        String windowKey = key + ":" + windowId;

        WindowState state = windows.computeIfAbsent(windowKey,
                k -> new WindowState(windowId, config.numShards()));

        // Atomically count this request locally
        int localTotal = state.localCount.incrementAndGet();

        // Estimate the global count: known distributed count + requests not yet flushed
        int unflushed = localTotal - state.flushedCount.get();
        int estimatedGlobal = state.estimatedGlobalCount() + unflushed;

        // Dynamically cap batch size relative to the limit to bound overshoot.
        // For small limits (e.g., 50), this ensures we flush frequently enough to
        // maintain reasonable accuracy across the fleet.
        int effectiveBatch = Math.max(1, Math.min(config.batchSize(), limit / OVERSHOOT_DIVISOR));

        if (unflushed >= effectiveBatch) {
            triggerFlush(windowKey, state);
        }

        return CompletableFuture.completedFuture(estimatedGlobal <= limit);
    }

    /**
     * Attempts a non-blocking flush of accumulated local counts to the distributed store.
     * <p>
     * Uses CAS on {@link WindowState#flushInProgress} to ensure only one flush is in-flight
     * per window key at a time. The delta is written to the next shard key in round-robin
     * order to distribute write load.
     */
    private void triggerFlush(String windowKey, WindowState state) {
        // Only one in-flight flush per window at a time
        if (!state.flushInProgress.compareAndSet(false, true)) {
            return;
        }

        int snapshotLocal = state.localCount.get();
        int snapshotFlushed = state.flushedCount.get();
        int delta = snapshotLocal - snapshotFlushed;

        if (delta <= 0) {
            state.flushInProgress.set(false);
            return;
        }

        // Round-robin across shards to spread write load
        int shard = state.nextShard();
        String shardKey = windowKey + ":" + shard;
        int expiration = config.windowSeconds() + EXPIRATION_BUFFER_SECONDS;

        try {
            store.incrementByAndExpire(shardKey, delta, expiration)
                    .whenComplete((shardCount, error) -> {
                        try {
                            if (error == null && shardCount != null) {
                                // Advance the flushed watermark and update this shard's known count.
                                // Requests arriving between snapshotLocal and now remain unflushed
                                // and will be picked up by the next flush cycle.
                                state.flushedCount.set(snapshotLocal);
                                state.lastKnownShardCounts.set(shard, shardCount);
                            }
                            // On failure: flushedCount stays unchanged, so the full delta
                            // (plus any new requests) will be retried on the next flush.
                        } finally {
                            state.flushInProgress.set(false);
                        }
                    });
        } catch (Exception e) {
            // incrementByAndExpire threw synchronously (e.g., connection refused).
            // Release the flush lock so the next threshold crossing can retry.
            state.flushInProgress.set(false);
        }
    }

    /**
     * Background task executed periodically to:
     * <ol>
     *   <li>Flush any pending local counts that haven't reached the batch threshold
     *       (ensures low-traffic keys still report their counts to the fleet).</li>
     *   <li>Remove expired window entries to prevent memory leaks.</li>
     * </ol>
     */
    void periodicSync() {
        long currentWindow = currentWindowId();

        windows.forEach((key, state) -> {
            if (state.windowId == currentWindow) {
                int unflushed = state.localCount.get() - state.flushedCount.get();
                if (unflushed > 0) {
                    triggerFlush(key, state);
                }
            }
        });

        // Purge windows that are no longer active (keep current and previous for safety)
        windows.entrySet().removeIf(entry -> entry.getValue().windowId < currentWindow - 1);
    }

    private long currentWindowId() {
        return clock.millis() / (config.windowSeconds() * 1000L);
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // Best-effort final flush of all pending counts
        windows.forEach(this::triggerFlush);
    }

    // -------------------------------------------------------------------------
    //  Per-window, per-key state tracked locally on this server instance
    // -------------------------------------------------------------------------

    static class WindowState {
        final long windowId;
        /** Total requests counted locally in this window (monotonically increasing). */
        final AtomicInteger localCount = new AtomicInteger(0);
        /** How many of {@code localCount} have been flushed to the distributed store. */
        final AtomicInteger flushedCount = new AtomicInteger(0);
        /** Last known count returned by the store for each shard key. */
        final AtomicIntegerArray lastKnownShardCounts;
        /** Guards against concurrent flushes for this window. */
        final AtomicBoolean flushInProgress = new AtomicBoolean(false);

        private final AtomicInteger shardCounter = new AtomicInteger(0);
        private final int numShards;

        WindowState(long windowId, int numShards) {
            this.windowId = windowId;
            this.numShards = numShards;
            this.lastKnownShardCounts = new AtomicIntegerArray(numShards);
        }

        int nextShard() {
            // floorMod guarantees a non-negative result even after int overflow
            return Math.floorMod(shardCounter.getAndIncrement(), numShards);
        }

        /**
         * Returns the best-known global count by summing the last observed value
         * from each shard. This may slightly lag the true distributed total, making
         * the limiter marginally more permissive between flushes — which is acceptable
         * per the "at least" guarantee.
         */
        int estimatedGlobalCount() {
            int sum = 0;
            for (int i = 0; i < lastKnownShardCounts.length(); i++) {
                sum += lastKnownShardCounts.get(i);
            }
            return sum;
        }
    }
}

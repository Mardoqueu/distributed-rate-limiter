package com.ratelimiter.config;

/**
 * Configuration for the distributed rate limiter.
 *
 * @param batchSize      max local requests accumulated before flushing to the distributed store
 * @param syncIntervalMs interval in milliseconds for periodic background flushes
 * @param windowSeconds  the fixed rate-limit window duration in seconds
 * @param numShards      number of key shards used to distribute write load across the store
 */
public record RateLimiterConfig(
        int batchSize,
        int syncIntervalMs,
        int windowSeconds,
        int numShards
) {
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_SYNC_INTERVAL_MS = 1000;
    public static final int DEFAULT_WINDOW_SECONDS = 60;
    public static final int DEFAULT_NUM_SHARDS = 10;

    public RateLimiterConfig {
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        if (syncIntervalMs < 1) throw new IllegalArgumentException("syncIntervalMs must be >= 1");
        if (windowSeconds < 1) throw new IllegalArgumentException("windowSeconds must be >= 1");
        if (numShards < 1) throw new IllegalArgumentException("numShards must be >= 1");
    }

    public static RateLimiterConfig defaults() {
        return new RateLimiterConfig(
                DEFAULT_BATCH_SIZE,
                DEFAULT_SYNC_INTERVAL_MS,
                DEFAULT_WINDOW_SECONDS,
                DEFAULT_NUM_SHARDS
        );
    }
}

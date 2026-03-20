package com.ratelimiter.store;

import java.util.concurrent.CompletableFuture;

/**
 * Existing data access layer for distributed key-value operations.
 * <p>
 * Each call results in a network round-trip to the underlying distributed store.
 * The actual implementation is provided by the infrastructure; this class serves
 * as the contract described in the problem statement.
 */
public class DistributedKeyValueStore {

    /**
     * Atomically increments the value of a key by the given delta and sets an expiration.
     * <p>
     * If the key does not exist, it is initialized to zero before incrementing.
     * The expiration is only applied when the key is first created; subsequent calls
     * ignore the {@code expirationSeconds} parameter until the key expires and is recreated.
     *
     * @param key               the key to increment
     * @param delta             the amount to increment by
     * @param expirationSeconds the TTL in seconds, applied only on key creation
     * @return a future containing the new value after incrementing
     * @throws Exception if the operation fails synchronously
     */
    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds)
            throws Exception {
        throw new UnsupportedOperationException("Stub — actual implementation provided by infrastructure");
    }
}

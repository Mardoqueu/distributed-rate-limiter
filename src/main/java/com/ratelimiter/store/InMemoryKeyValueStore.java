package com.ratelimiter.store;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory implementation of {@link DistributedKeyValueStore} for local testing and demos.
 * <p>
 * Faithfully emulates the contract: keys are initialized to zero on first access,
 * expiration is scheduled only on creation, and subsequent calls ignore the TTL
 * until the key expires and is recreated.
 */
public class InMemoryKeyValueStore extends DistributedKeyValueStore {

    private final ConcurrentHashMap<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService expiryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kv-store-expiry");
        t.setDaemon(true);
        return t;
    });

    @Override
    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds) {
        AtomicInteger counter = counters.computeIfAbsent(key, k -> {
            // Schedule automatic removal — only fires on key creation, matching the real contract
            expiryScheduler.schedule(() -> counters.remove(k), expirationSeconds, TimeUnit.SECONDS);
            return new AtomicInteger(0);
        });
        int newValue = counter.addAndGet(delta);
        return CompletableFuture.completedFuture(newValue);
    }
}

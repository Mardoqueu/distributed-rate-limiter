package com.ratelimiter.limiter;

import com.ratelimiter.config.RateLimiterConfig;
import com.ratelimiter.store.DistributedKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DistributedHighThroughputRateLimiterTest {

    @Mock
    private DistributedKeyValueStore store;

    private MutableClock clock;
    private DistributedHighThroughputRateLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(60_000L); // start at a clean window boundary
    }

    @AfterEach
    void tearDown() {
        if (limiter != null) {
            limiter.close();
        }
    }

    // ------------------------------------------------------------------
    //  Core behavior
    // ------------------------------------------------------------------

    @Test
    void shouldAllowRequestsUnderLimit() throws Exception {
        limiter = createLimiter(10, 1);
        stubStoreReturning(5);

        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.isAllowed("client1", 100).join(),
                    "Request " + i + " should be allowed");
        }
    }

    @Test
    void shouldDenyWhenLocalEstimateExceedsLimit() throws Exception {
        // With 1 shard and batch size 10, the effective batch for limit=10 is
        // max(1, min(10, 10/10)) = 1. So every request triggers a flush.
        limiter = createLimiter(10, 1);
        stubStoreReturning(10); // store says count is already at the limit

        // First request: localCount=1, unflushed=1 >= effectiveBatch(1), triggers flush.
        // estimatedGlobal = 0 (no shard data yet) + 1 = 1 <= 10 → true
        assertTrue(limiter.isAllowed("client1", 10).join());

        // Wait for the flush to reach the store, confirming shard counts are updated
        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), anyInt(), anyInt());

        // Now the shard reports 10. localCount=2, flushedCount=1, unflushed=1.
        // estimatedGlobal = 10 + 1 = 11 > 10 → false
        assertFalse(limiter.isAllowed("client1", 10).join());
    }

    @Test
    void shouldDenyAfterFlushRevealsGlobalCountAboveLimit() throws Exception {
        limiter = createLimiter(5, 1);

        // Simulate a realistic store that accumulates counts and eventually exceeds the limit.
        // Other servers have already contributed 480 requests to the same window.
        AtomicInteger storeCount = new AtomicInteger(480);
        CountDownLatch flushLatch = new CountDownLatch(1);
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenAnswer(inv -> {
                    int delta = inv.getArgument(1);
                    int newVal = storeCount.addAndGet(delta);
                    if (newVal >= 500) {
                        flushLatch.countDown();
                    }
                    return CompletableFuture.completedFuture(newVal);
                });

        // effectiveBatch = min(5, 500/10) = 5
        // After 5 requests: flush sends delta=5, store returns 485.
        // Continue sending until the store count reaches the limit.
        for (int i = 0; i < 25; i++) {
            limiter.isAllowed("client1", 500).join();
        }

        // Wait for flushes to propagate the store count past the limit
        assertTrue(flushLatch.await(2, TimeUnit.SECONDS),
                "Store count should eventually reach the limit");

        // Now the next request should be denied since estimatedGlobal > 500
        boolean deniedAtLeastOnce = false;
        for (int i = 0; i < 10; i++) {
            if (!limiter.isAllowed("client1", 500).join()) {
                deniedAtLeastOnce = true;
                break;
            }
        }
        assertTrue(deniedAtLeastOnce, "Should eventually deny requests once global count exceeds limit");
    }

    // ------------------------------------------------------------------
    //  Batching behavior
    // ------------------------------------------------------------------

    @Test
    void shouldNotCallStoreBeforeBatchThresholdReached() throws Exception {
        limiter = createLimiter(10, 1);

        // limit=1000, effectiveBatch = min(10, 1000/10) = 10.
        // 9 requests should NOT trigger a flush.
        for (int i = 0; i < 9; i++) {
            limiter.isAllowed("client1", 1000).join();
        }

        verifyNoInteractions(store);
    }

    @Test
    void shouldFlushToStoreWhenBatchThresholdReached() throws Exception {
        limiter = createLimiter(10, 1);
        stubStoreReturning(10);

        // 10th request triggers a flush with delta = 10
        for (int i = 0; i < 10; i++) {
            limiter.isAllowed("client1", 1000).join();
        }

        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), eq(10), eq(65));
    }

    @Test
    void shouldCapBatchSizeForSmallLimits() throws Exception {
        limiter = createLimiter(100, 1);
        stubStoreReturning(1);

        // limit=20, effectiveBatch = max(1, min(100, 20/10)) = 2
        limiter.isAllowed("client1", 20).join();
        verifyNoInteractions(store);

        limiter.isAllowed("client1", 20).join();
        // Should flush after 2 requests with delta=2
        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), eq(2), eq(65));
    }

    // ------------------------------------------------------------------
    //  Sharding
    // ------------------------------------------------------------------

    @Test
    void shouldDistributeFlushesAcrossShards() throws Exception {
        int numShards = 3;
        limiter = createLimiter(5, numShards);
        stubStoreReturning(5);

        // Trigger 3 flushes (each batch of 5 requests, limit large enough for batch=5)
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 5; i++) {
                limiter.isAllowed("client1", 10_000).join();
            }
            // Wait for each flush to complete before triggering the next
            verify(store, timeout(500).atLeast(batch + 1))
                    .incrementByAndExpire(anyString(), anyInt(), anyInt());
        }

        // Verify the store was called with 3 different shard keys
        verify(store, timeout(500).atLeast(3))
                .incrementByAndExpire(anyString(), anyInt(), eq(65));

        // Capture the actual keys to verify they include different shard suffixes
        var keyCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(store, atLeast(3)).incrementByAndExpire(keyCaptor.capture(), anyInt(), anyInt());

        long distinctShards = keyCaptor.getAllValues().stream()
                .map(k -> k.substring(k.lastIndexOf(":") + 1))
                .distinct()
                .count();
        assertTrue(distinctShards >= 2, "Expected writes to at least 2 different shards, got " + distinctShards);
    }

    // ------------------------------------------------------------------
    //  Window management
    // ------------------------------------------------------------------

    @Test
    void shouldResetCountsOnNewWindow() throws Exception {
        limiter = createLimiter(5, 1);
        // Store returns 500 after flush — exactly at the limit
        stubStoreReturning(500);

        // Fill up the limit: 5 requests trigger a flush, store returns 500
        for (int i = 0; i < 5; i++) {
            limiter.isAllowed("client1", 500).join();
        }
        // Wait for the flush to complete
        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), anyInt(), anyInt());

        // Should be denied now: estimatedGlobal = 500 (from store) + 1 (unflushed) = 501 > 500
        assertFalse(limiter.isAllowed("client1", 500).join());

        // Advance clock to a new window
        clock.advance(61_000);

        // New window: fresh WindowState, counts reset — should be allowed again
        assertTrue(limiter.isAllowed("client1", 500).join());
    }

    // ------------------------------------------------------------------
    //  Error handling
    // ------------------------------------------------------------------

    @Test
    void shouldContinueOperatingWhenStoreReturnsFailedFuture() throws Exception {
        limiter = createLimiter(5, 1);
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("store timeout")));

        // Even with store failures, isAllowed should not throw and should fail open
        // (allow requests based on local estimate)
        for (int i = 0; i < 20; i++) {
            assertDoesNotThrow(() -> limiter.isAllowed("client1", 1000).join());
        }
    }

    @Test
    void shouldContinueOperatingWhenStoreThrowsSynchronously() throws Exception {
        limiter = createLimiter(5, 1);
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenThrow(new RuntimeException("connection refused"));

        for (int i = 0; i < 20; i++) {
            assertDoesNotThrow(() -> limiter.isAllowed("client1", 1000).join());
        }
    }

    @Test
    void shouldRetryDeltaAfterStoreFailure() throws Exception {
        limiter = createLimiter(5, 1);

        // First flush fails
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("down")));

        // effectiveBatch = min(5, 1000/10) = 5
        for (int i = 0; i < 5; i++) {
            limiter.isAllowed("client1", 1000).join();
        }
        // Wait for the failed flush to complete and release the lock
        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), anyInt(), anyInt());

        // Fix the store — next flush should include the unflushed delta from the failed batch
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(10));

        for (int i = 0; i < 5; i++) {
            limiter.isAllowed("client1", 1000).join();
        }

        verify(store, timeout(500).atLeast(2))
                .incrementByAndExpire(anyString(), anyInt(), eq(65));

        // The second (successful) flush should carry a delta > 5, because it includes
        // the requests from the failed batch that were never committed.
        var deltaCaptor = org.mockito.ArgumentCaptor.forClass(int.class);
        verify(store, atLeast(2)).incrementByAndExpire(anyString(), deltaCaptor.capture(), anyInt());

        var deltas = deltaCaptor.getAllValues();
        int retryDelta = deltas.get(deltas.size() - 1);
        assertTrue(retryDelta > 5,
                "Retry flush should include accumulated delta from failed batch, got " + retryDelta);
    }

    // ------------------------------------------------------------------
    //  Concurrency
    // ------------------------------------------------------------------

    @Test
    void shouldHandleConcurrentAccessWithoutErrors() throws Exception {
        limiter = createLimiter(50, 5);
        stubStoreReturning(50);

        int numThreads = 10;
        int requestsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads * requestsPerThread);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                for (int i = 0; i < requestsPerThread; i++) {
                    try {
                        limiter.isAllowed("client1", 5000).join();
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "All requests should complete within timeout");
        executor.shutdown();
        assertEquals(0, errors.get(), "No errors should occur under concurrent access");
    }

    @Test
    void shouldMaintainApproximateAccuracyUnderConcurrency() throws Exception {
        // Use 1 shard for predictable counting in this test
        limiter = createLimiter(100, 1);
        AtomicInteger storeCounter = new AtomicInteger(0);

        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenAnswer(invocation -> {
                    int delta = invocation.getArgument(1);
                    int newCount = storeCounter.addAndGet(delta);
                    return CompletableFuture.completedFuture(newCount);
                });

        int numThreads = 8;
        int requestsPerThread = 500;
        int totalRequests = numThreads * requestsPerThread;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(totalRequests);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                for (int i = 0; i < requestsPerThread; i++) {
                    try {
                        limiter.isAllowed("client1", 100_000).join();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Force a final flush via periodicSync to push remaining counts
        limiter.periodicSync();
        // Wait for all flushed counts to arrive at the store
        verify(store, timeout(1000).atLeast(1))
                .incrementByAndExpire(anyString(), anyInt(), anyInt());

        // The store should have received approximately all requests
        // (some may remain unflushed locally — allow a one-batch margin)
        int flushed = storeCounter.get();
        assertTrue(flushed >= totalRequests - 100,
                "Store should have received most counts. Expected ~" + totalRequests + ", got " + flushed);
        assertTrue(flushed <= totalRequests,
                "Store should not receive more counts than actual requests. Got " + flushed);
    }

    // ------------------------------------------------------------------
    //  Periodic sync
    // ------------------------------------------------------------------

    @Test
    void shouldFlushPendingCountsOnPeriodicSync() throws Exception {
        limiter = createLimiter(100, 1); // batch size 100 — won't be reached in this test
        stubStoreReturning(3);

        // Only 3 requests — well below the batch threshold
        for (int i = 0; i < 3; i++) {
            limiter.isAllowed("client1", 1000).join();
        }
        verifyNoInteractions(store);

        // Periodic sync should flush the 3 pending requests
        limiter.periodicSync();

        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), eq(3), eq(65));
    }

    @Test
    void shouldCleanUpExpiredWindowsOnPeriodicSync() throws Exception {
        limiter = createLimiter(5, 1);
        stubStoreReturning(5);

        // Create entries in the current window
        for (int i = 0; i < 5; i++) {
            limiter.isAllowed("client1", 1000).join();
        }
        // Wait for the flush to complete
        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), anyInt(), anyInt());

        // Advance clock by 2 full windows so the old window is eligible for cleanup
        clock.advance(121_000);

        // Trigger cleanup
        limiter.periodicSync();

        // Make a request in the new window to verify the limiter still works
        assertTrue(limiter.isAllowed("client1", 1000).join());
    }

    // ------------------------------------------------------------------
    //  Throughput
    // ------------------------------------------------------------------

    @Tag("performance")
    @Test
    void shouldSupportHighThroughputOnHotPath() throws Exception {
        limiter = createLimiter(1000, 10);
        stubStoreReturning(1);

        int iterations = 1_000_000;
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            limiter.isAllowed("client1", 100_000).join();
        }
        long elapsedNs = System.nanoTime() - start;

        double opsPerSecond = iterations / (elapsedNs / 1_000_000_000.0);
        // At 100M req/min we need ~1.67M ops/sec. Single-threaded local decisions
        // (no I/O) should comfortably exceed this on modern hardware.
        assertTrue(opsPerSecond > 1_000_000,
                "Expected >1M ops/sec single-threaded, got " + String.format("%,.0f", opsPerSecond));
        System.out.printf("Throughput: %,.0f ops/sec (single-threaded)%n", opsPerSecond);
    }

    // ------------------------------------------------------------------
    //  Input validation
    // ------------------------------------------------------------------

    @Test
    void shouldRejectNullKey() {
        limiter = createLimiter(10, 1);
        assertThrows(NullPointerException.class, () -> limiter.isAllowed(null, 100));
    }

    @Test
    void shouldRejectNegativeLimit() {
        limiter = createLimiter(10, 1);
        assertThrows(IllegalArgumentException.class, () -> limiter.isAllowed("client1", -1));
    }

    @Test
    void shouldHandleZeroLimit() throws Exception {
        limiter = createLimiter(10, 1);
        // limit=0 means no requests allowed. effectiveBatch = max(1, min(10, 0/10)) = 1
        assertFalse(limiter.isAllowed("client1", 0).join());
    }

    // ------------------------------------------------------------------
    //  Lifecycle
    // ------------------------------------------------------------------

    @Test
    void shouldFlushPendingCountsOnClose() throws Exception {
        limiter = createLimiter(100, 1); // large batch — won't flush on its own
        stubStoreReturning(7);

        for (int i = 0; i < 7; i++) {
            limiter.isAllowed("client1", 10_000).join();
        }
        verifyNoInteractions(store);

        // close() should trigger a best-effort final flush
        limiter.close();
        limiter = null; // prevent double-close in tearDown

        verify(store, timeout(500).times(1))
                .incrementByAndExpire(anyString(), eq(7), eq(65));
    }

    // ------------------------------------------------------------------
    //  Multiple keys
    // ------------------------------------------------------------------

    @Test
    void shouldTrackMultipleKeysIndependently() throws Exception {
        limiter = createLimiter(5, 1);
        stubStoreReturning(5);

        // Send 5 requests per key — only enough to trigger one flush per key
        for (int i = 0; i < 5; i++) {
            limiter.isAllowed("clientA", 10_000).join();
            limiter.isAllowed("clientB", 10_000).join();
        }

        // Both keys should have triggered independent flushes
        verify(store, timeout(500).atLeast(2))
                .incrementByAndExpire(anyString(), eq(5), eq(65));

        // Verify we see keys for both clients
        var keyCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(store, atLeast(2)).incrementByAndExpire(keyCaptor.capture(), anyInt(), anyInt());

        long distinctClients = keyCaptor.getAllValues().stream()
                .filter(k -> k.startsWith("clientA") || k.startsWith("clientB"))
                .map(k -> k.startsWith("clientA") ? "A" : "B")
                .distinct()
                .count();
        assertEquals(2, distinctClients, "Both client keys should have been flushed independently");
    }

    @Test
    void shouldHandleConcurrentMultipleKeys() throws Exception {
        limiter = createLimiter(50, 5);
        stubStoreReturning(50);

        int numKeys = 5;
        int numThreads = 10;
        int requestsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads * requestsPerThread);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final String key = "client" + (t % numKeys);
            executor.submit(() -> {
                for (int i = 0; i < requestsPerThread; i++) {
                    try {
                        limiter.isAllowed(key, 50_000).join();
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "All requests should complete within timeout");
        executor.shutdown();
        assertEquals(0, errors.get(), "No errors should occur with concurrent multi-key access");
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private DistributedHighThroughputRateLimiter createLimiter(int batchSize, int numShards) {
        // Use a very long sync interval so the scheduler never fires during tests
        RateLimiterConfig config = new RateLimiterConfig(batchSize, 600_000, 60, numShards);
        return new DistributedHighThroughputRateLimiter(store, config, clock);
    }

    private void stubStoreReturning(int value) throws Exception {
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(value));
    }

    /**
     * A mutable clock for deterministic testing of time-dependent behavior.
     */
    static class MutableClock extends Clock {
        private final AtomicLong currentMillis;

        MutableClock(long initialMillis) {
            this.currentMillis = new AtomicLong(initialMillis);
        }

        void advance(long millis) {
            currentMillis.addAndGet(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(currentMillis.get());
        }

        @Override
        public long millis() {
            return currentMillis.get();
        }
    }
}

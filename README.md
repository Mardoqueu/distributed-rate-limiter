# Design Discussion: DistributedHighThroughputRateLimiter

This document covers edge cases, error handling, testing strategy, code organization,
maintainability, extensibility, and the key trade-offs behind the implementation.

---

## 1. Edge Cases

### 1.1 Zero Limit

When `limit = 0` the effective batch size becomes `max(1, min(batchSize, 0/10)) = 1`.
The very first request yields `estimatedGlobal = 0 + 1 = 1 > 0`, so it is immediately
denied. No special-case branch is needed — the general formula handles it naturally.

### 1.2 Very Small Limits

For a limit of 20 with a default batch size of 100, the effective batch is capped at
`20 / 10 = 2`. This forces a flush every 2 local requests, keeping the per-server
overshoot to at most 2 — a fraction of the configured limit. Without this cap, the
system could allow up to 100 extra requests per server before the distributed store
was even consulted.

### 1.3 Window Boundary Transitions

Each 60-second window uses a unique key prefix (`key:windowId`). When the clock
crosses a window boundary, a fresh `WindowState` is created with all counters at
zero. The old window's keys expire automatically via TTL in the store. This means:

- **No stale carry-over:** counts from the previous window never leak into the new one.
- **Boundary burst:** a client can issue requests at the end of window N and the start
  of window N+1, theoretically reaching up to 2x the limit in a short span. This is a
  known trade-off of fixed-window algorithms (see Section 5.1).

### 1.4 Integer Overflow in Shard Counter

The `shardCounter` (an `AtomicInteger`) is incremented on every flush. Over time, it
will wrap past `Integer.MAX_VALUE`. The shard index is computed as
`Math.floorMod(shardCounter.getAndIncrement(), numShards)`, which always returns a
non-negative value even after overflow — avoiding `ArrayIndexOutOfBoundsException`.

### 1.5 Null and Negative Inputs

`isAllowed` rejects `null` keys with `NullPointerException` and negative limits with
`IllegalArgumentException` at the top of the method, before any state is touched.
These are fail-fast guards that prevent subtle corruption of internal data structures.

### 1.6 Clock Skew Across Fleet

The system uses `Clock.systemUTC()` for window alignment. If clocks differ across
servers by more than a few seconds, different servers may be writing to different
window keys simultaneously. The impact is bounded: requests are still counted, just
in adjacent windows. For systems where clock skew is significant, NTP synchronization
is a prerequisite. The 5-second `EXPIRATION_BUFFER_SECONDS` on TTLs provides a small
safety margin.

### 1.7 Server Crash (Abrupt Termination)

If a server is killed without calling `close()` (e.g., `kill -9`, OOM, hardware
failure), any unflushed local counts are lost. The distributed store never receives
those increments, so the global count temporarily under-reports.

**Impact:** the fleet becomes slightly more permissive for the remainder of the
window — consistent with the "at least" guarantee. The maximum loss per crash is
bounded by `effectiveBatch` requests (the most that can accumulate locally before
a flush is triggered).

**Mitigation:** reducing `syncIntervalMs` (e.g., from 1000ms to 200ms) narrows the
window of data loss at the cost of more frequent background flushes. In practice,
server crashes are rare events and the lost delta is negligible relative to the
configured limit.

### 1.8 TTL Expiration During In-Flight Flush

A flush is asynchronous: the delta is sent to the store and the response arrives
later. If the shard key expires in the store between the send and the response, the
store contract reinitializes the key to zero before applying the delta. The returned
value reflects only this server's delta, not the full accumulated count.

This can happen near the end of a window when the TTL (60s + 5s buffer = 65s) is
close to expiring. The effect is a momentary undercount for that shard — the system
becomes slightly more permissive. Since this only occurs at window boundaries (where
a new window is about to start anyway), the practical impact is negligible.

### 1.9 Concurrent Read-Flush Race on the Hot Path

In `isAllowed`, there is a deliberate gap between `localCount.incrementAndGet()`
and `flushedCount.get()`. Between these two atomic reads, another thread can complete
a flush that advances `flushedCount` and updates `lastKnownShardCounts`. This means:

- `unflushed` may momentarily undercount (flush advanced `flushedCount` but we
  already read `estimatedGlobalCount` before the shard update).
- `unflushed` may momentarily overcount (we read old `flushedCount` but the shard
  count already reflects the flushed delta).

Both directions are bounded by one batch size and self-correct on the next call.
The alternative — using a lock to make the read-check atomic — would serialize the
hot path and degrade throughput by orders of magnitude. Since the requirement permits
approximate counting, this race is an acceptable trade-off for lock-free performance.

### 1.10 High Cardinality Keys

Each unique `(key, windowId)` pair allocates a `WindowState` in a `ConcurrentHashMap`.
If millions of distinct client IDs appear, memory grows linearly. The `periodicSync`
background task purges entries older than `currentWindow - 1`, preventing unbounded
growth over time. Within a single window, memory is proportional to the number of
active clients — the same as any per-client rate limiter.

---

## 2. Error Handling

### 2.1 Design Principle: Fail-Open

The rate limiter is designed to **fail-open** — when the distributed store is
unreachable, requests are allowed based on the local estimate rather than being
rejected. This is a deliberate choice: in a high-throughput system, a transient store
outage should not cause a global denial of service to all clients.

### 2.2 Asynchronous Store Failures (`CompletableFuture` failure)

When `incrementByAndExpire` returns a failed `CompletableFuture`:

```java
.whenComplete((shardCount, error) -> {
    try {
        if (error == null && shardCount != null) {
            state.flushedCount.set(snapshotLocal);
            state.lastKnownShardCounts.set(shard, shardCount);
        }
        // On failure: flushedCount stays unchanged, so the full delta
        // (plus any new requests) will be retried on the next flush.
    } finally {
        state.flushInProgress.set(false);
    }
});
```

- `flushedCount` is **not advanced**, so the unflushed delta accumulates.
- `flushInProgress` is released, allowing the next threshold crossing to retry.
- The caller (`isAllowed`) is never blocked or affected — it already returned.

### 2.3 Synchronous Store Exceptions

If `incrementByAndExpire` throws synchronously (e.g., connection refused before
a future is even created), the outer `try/catch` in `triggerFlush` catches it and
releases `flushInProgress`:

```java
} catch (Exception e) {
    state.flushInProgress.set(false);
}
```

This prevents the flush lock from being permanently held, which would stop all
future flushes for that window.

### 2.4 Retry Semantics

Failed deltas are not lost. Because `flushedCount` is only advanced on success,
the next flush naturally includes the previously failed delta plus any new requests
that arrived in the meantime. This is verified by the `shouldRetryDeltaAfterStoreFailure`
test, which asserts that the retry delta exceeds the batch size.

### 2.5 Graceful Shutdown

`close()` shuts down the scheduler and performs a best-effort final flush of all
pending window states. The `finally` block re-interrupts the thread if
`awaitTermination` is interrupted, preserving the interruption contract.

---

## 3. Unit Tests

### 3.1 Test Organization

Tests are grouped by concern with section headers:

| Section | Tests | Purpose |
|---|---|---|
| Core behavior | 3 | Allow/deny decisions under various conditions |
| Batching | 3 | Flush thresholds, batch capping for small limits |
| Sharding | 1 | Round-robin distribution across shard keys |
| Window management | 1 | Count reset on new 60-second window |
| Error handling | 3 | Failed futures, synchronous exceptions, retry semantics |
| Concurrency | 2 | Multi-threaded safety and approximate accuracy |
| Periodic sync | 2 | Background flush of sub-threshold counts, expired window cleanup |
| Lifecycle | 1 | `close()` triggers final flush |
| Throughput | 1 | >1M ops/sec single-threaded performance |
| Input validation | 3 | Null key, negative limit, zero limit |
| Multiple keys | 2 | Key isolation and concurrent multi-key access |
| Controller | 5 | HTTP status codes, default params, missing params |

**Total: 27 tests** across two test classes.

### 3.2 Test Infrastructure

- **`MutableClock`**: extends `java.time.Clock` with an `advance(millis)` method.
  Allows deterministic control of window boundaries without `Thread.sleep`.
- **Mockito stubs**: `DistributedKeyValueStore` is mocked in unit tests to avoid
  coupling to any real store implementation. Stubs are configured per test to
  simulate specific scenarios (store returning a value, failing, accumulating).
- **`CountDownLatch`**: used for synchronizing multi-threaded tests instead of
  polling or sleeping.
- **`verify(..., timeout(...))`**: Mockito's built-in timeout verification replaces
  `Thread.sleep` for waiting on asynchronous flush completion. This is more reliable
  in CI environments with variable scheduling latency.
- **`@Tag("performance")`**: the throughput benchmark is tagged so it can be excluded
  from fast CI runs (`-Dgroups="!performance"`).

### 3.3 Controller Tests

`RateLimiterControllerTest` uses Spring's `@WebMvcTest` slice to test the HTTP layer
in isolation, with the rate limiter mocked via `@MockitoBean`. This validates:

- Correct HTTP status codes (200 vs 429)
- JSON response structure
- Default parameter handling
- Missing required parameter handling

---

## 4. Maintainability and Extensibility

### 4.1 Code Organization

```
com.ratelimiter
├── Application.java                          # Entry point
├── config/
│   ├── RateLimiterConfig.java                # Immutable config (record)
│   └── RateLimiterBeanConfig.java            # Spring wiring
├── controller/
│   └── RateLimiterController.java            # REST API
├── limiter/
│   └── DistributedHighThroughputRateLimiter  # Core algorithm
└── store/
    ├── DistributedKeyValueStore.java         # Store contract (given)
    └── InMemoryKeyValueStore.java            # Test/demo implementation
```

Packages are organized by layer (config, controller, limiter, store), not by
technical pattern. Each package has a clear responsibility and minimal coupling
to others.

### 4.2 Configuration as a Record

`RateLimiterConfig` is a Java record with:

- **Immutability**: once created, configuration cannot change.
- **Validation**: compact constructor rejects invalid values at construction time.
- **Defaults**: `RateLimiterConfig.defaults()` provides sensible production values.
- **Extensibility**: adding a new parameter (e.g., `maxRetries`) requires only adding
  a field to the record and a validation line — no builder or setter boilerplate.

### 4.3 Clock Injection

The rate limiter accepts a `java.time.Clock` via a package-private constructor. This
enables deterministic testing without modifying production code. The public constructor
defaults to `Clock.systemUTC()`.

### 4.4 AutoCloseable

Implementing `AutoCloseable` ensures the scheduler is properly shut down, whether the
limiter is used in a try-with-resources block, managed by Spring's `destroyMethod`,
or closed manually. This prevents thread leaks in tests and application restarts.

### 4.5 Extending the Store

The `DistributedKeyValueStore` class (provided by the problem statement) can be
subclassed to integrate with any backend: Redis, DynamoDB, Memcached, etc. The
`InMemoryKeyValueStore` demonstrates this — it faithfully emulates the contract
(atomic increment, TTL only on creation) for local testing and demos.

### 4.6 Extending the Algorithm

The main extension points without modifying core logic:

| Change | How |
|---|---|
| Different window duration | Set `windowSeconds` in `RateLimiterConfig` |
| More/fewer shards | Set `numShards` in `RateLimiterConfig` |
| Larger/smaller batches | Set `batchSize` in `RateLimiterConfig` |
| Different store backend | Subclass `DistributedKeyValueStore` |
| Sliding window algorithm | Replace `currentWindowId()` logic and add dual-window counting |
| Per-key config (e.g., limits from a database) | Already supported — `limit` is a parameter of `isAllowed`, not config |

---

## 5. Trade-offs

### 5.1 Fixed Window vs. Sliding Window

**Chosen:** Fixed 60-second windows aligned to clock time.

**Trade-off:** A burst of requests straddling two window boundaries can allow up to
2x the configured limit within a short timespan (e.g., 500 requests in the last
second of window N, then 500 more in the first second of window N+1).

**Why this is acceptable:**

- The problem states "approximately 500 requests in the last 60 seconds" — the word
  "approximately" signals tolerance for imprecision.
- A sliding window would require reading multiple keys from the store (current window +
  previous window) on every flush, doubling store calls and complicating the sharding
  strategy.
- For rate limiting (as opposed to billing), brief overshoot at boundaries is
  rarely harmful.

**Alternative:** A dual-counter approach (tracking both current and previous window,
weighting by elapsed time) could reduce boundary burst to ~1.5x with minimal
additional complexity. This would be the natural next step if tighter accuracy
were required.

### 5.2 Local Batching: Accuracy vs. Throughput

**Chosen:** Flush to the store only after `effectiveBatch` local requests accumulate.

**Trade-off:** Between flushes, each server is unaware of counts from other servers.
The maximum overshoot across the fleet is bounded by `numServers × effectiveBatch`.
With 10 servers and a batch size of 100, that's up to 1,000 extra requests beyond
the limit.

**Why this is acceptable:**

- The requirement explicitly states: "Clients must be able to make at least the amount
  of calls specified in their configured limit, but may temporarily make a little more."
- At 100M req/min, calling the store on every request (1.67M writes/sec to a single
  key) would overwhelm any distributed store. Batching reduces this by 100x.
- The `effectiveBatch` cap (`limit / OVERSHOOT_DIVISOR`) ensures overshoot stays
  proportional to the limit. A limit of 50 produces a batch of 5, not 100.

### 5.3 Key Sharding: Complexity vs. Hot-Key Prevention

**Chosen:** Flush writes are distributed across `numShards` shard keys in round-robin
order.

**Trade-off:** The global count estimate is the sum of last-known shard values, which
may lag the true total if other servers have written to shards that this server hasn't
observed. This makes the estimate more permissive (under-count), which is the correct
direction for the "at least" guarantee.

**Why this is necessary:**

- A single key receiving millions of increments per minute would create a hot partition
  in any distributed store (Redis, DynamoDB, etc.).
- The problem explicitly states: "the DistributedKeyValueStore will not handle those
  issues for you and will simply store and set an expiration on the exact key you specify."
- 10 shards reduce per-key write rate by 10x. Combined with batching (100x), the
  effective per-key write rate drops from 1.67M/sec to ~1,670/sec — well within the
  capacity of any production store.

### 5.4 Fail-Open vs. Fail-Closed

**Chosen:** When the store is unreachable, allow requests based on local estimate.

**Trade-off:** During a store outage, each server operates independently. If the outage
lasts an entire window, the fleet could collectively allow up to `numServers × limit`
requests instead of just `limit`.

**Why this is acceptable:**

- Rate limiting exists to protect downstream services from overload. If the rate
  limiter's own store is down, rejecting all traffic would cause a different kind of
  outage — a self-inflicted denial of service.
- Most rate limiting use cases prefer availability over precision. The alternative
  (fail-closed) would require every `isAllowed` call to block on a store round-trip,
  defeating the purpose of local batching.
- The unflushed delta is retried automatically on the next flush, so accuracy
  self-corrects once the store recovers.

### 5.5 Single Flush In-Flight vs. Concurrent Flushes

**Chosen:** Only one flush per `(key, window)` is allowed at a time, enforced by
CAS on `flushInProgress`.

**Trade-off:** If a flush is slow (high network latency), requests accumulate locally
without being reported to the store. The periodic sync (default every 1 second)
provides a safety net, but during the gap the estimate may lag.

**Why this is acceptable:**

- Concurrent flushes to the same window would create race conditions in updating
  `flushedCount` and `lastKnownShardCounts`. Resolving these races would require
  locks, degrading hot-path performance.
- The single-flush constraint is per window, not global. Different keys and different
  windows can flush concurrently.
- The periodic sync ensures that even under sustained load with slow flushes, counts
  are eventually reported.

### 5.6 CompletableFuture Return Type

**Chosen:** `isAllowed` returns `CompletableFuture<Boolean>`, but the future is always
already completed (`CompletableFuture.completedFuture(...)`).

**Trade-off:** The method signature implies asynchronous behavior, but the decision is
always synchronous (no I/O on the hot path).

**Why this is the right choice:**

- The problem statement requires this exact signature.
- Returning a completed future is zero-cost (no thread pool, no callback overhead).
- If a future version needed to consult the store before deciding (e.g., for a
  stricter accuracy mode), the API would not need to change.
- Callers can use `.join()` without risk of blocking, since the future is pre-resolved.

---

## 6. Performance Characteristics

| Metric | Value | Notes |
|---|---|---|
| Hot-path latency | < 1 microsecond | Single atomic increment + arithmetic |
| Single-threaded throughput | ~7.8M ops/sec | Measured on Apple Silicon (M-series) |
| Store calls per request | 1 / batchSize | Default: 1 per 100 requests |
| Store keys per window per client | numShards | Default: 10 |
| Memory per active client per window | ~120 bytes | WindowState with AtomicIntegerArray |
| Background threads | 1 | Daemon, single-threaded scheduler |

The system comfortably exceeds the 100M req/min requirement (1.67M ops/sec) by a
factor of ~4.7x on a single thread. With multiple threads, linear scaling is expected
since the hot path is lock-free.

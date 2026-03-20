package com.ratelimiter.controller;

import com.ratelimiter.limiter.DistributedHighThroughputRateLimiter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Demo endpoint to exercise the rate limiter interactively.
 * <p>
 * Usage:
 * <pre>
 *   curl "http://localhost:8080/api/check?clientId=xyz&amp;limit=500"
 * </pre>
 */
@RestController
@RequestMapping("/api")
public class RateLimiterController {

    private final DistributedHighThroughputRateLimiter rateLimiter;

    public RateLimiterController(DistributedHighThroughputRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @GetMapping("/check")
    public ResponseEntity<Map<String, Object>> checkRateLimit(
            @RequestParam String clientId,
            @RequestParam(defaultValue = "500") int limit) {

        boolean allowed = rateLimiter.isAllowed(clientId, limit).join();

        Map<String, Object> body = Map.of(
                "clientId", clientId,
                "allowed", allowed,
                "limit", limit
        );

        return allowed
                ? ResponseEntity.ok(body)
                : ResponseEntity.status(429).body(body);
    }
}

package com.ratelimiter.config;

import com.ratelimiter.limiter.DistributedHighThroughputRateLimiter;
import com.ratelimiter.store.DistributedKeyValueStore;
import com.ratelimiter.store.InMemoryKeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterBeanConfig {

    @Bean
    public DistributedKeyValueStore keyValueStore() {
        return new InMemoryKeyValueStore();
    }

    @Bean(destroyMethod = "close")
    public DistributedHighThroughputRateLimiter rateLimiter(DistributedKeyValueStore store) {
        return new DistributedHighThroughputRateLimiter(store, RateLimiterConfig.defaults());
    }
}

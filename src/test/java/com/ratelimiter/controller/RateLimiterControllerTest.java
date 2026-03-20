package com.ratelimiter.controller;

import com.ratelimiter.limiter.DistributedHighThroughputRateLimiter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RateLimiterController.class)
class RateLimiterControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DistributedHighThroughputRateLimiter rateLimiter;

    @Test
    void shouldReturn200WhenRequestIsAllowed() throws Exception {
        when(rateLimiter.isAllowed(eq("xyz"), eq(500)))
                .thenReturn(CompletableFuture.completedFuture(true));

        mockMvc.perform(get("/api/check")
                        .param("clientId", "xyz")
                        .param("limit", "500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clientId").value("xyz"))
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.limit").value(500));

        verify(rateLimiter).isAllowed("xyz", 500);
    }

    @Test
    void shouldReturn429WhenRequestIsDenied() throws Exception {
        when(rateLimiter.isAllowed(eq("xyz"), eq(500)))
                .thenReturn(CompletableFuture.completedFuture(false));

        mockMvc.perform(get("/api/check")
                        .param("clientId", "xyz")
                        .param("limit", "500"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.clientId").value("xyz"))
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.limit").value(500));
    }

    @Test
    void shouldUseDefaultLimitOf500WhenNotProvided() throws Exception {
        when(rateLimiter.isAllowed(eq("abc"), eq(500)))
                .thenReturn(CompletableFuture.completedFuture(true));

        mockMvc.perform(get("/api/check")
                        .param("clientId", "abc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limit").value(500));

        verify(rateLimiter).isAllowed("abc", 500);
    }

    @Test
    void shouldReturn400WhenClientIdIsMissing() throws Exception {
        mockMvc.perform(get("/api/check"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldAcceptCustomLimit() throws Exception {
        when(rateLimiter.isAllowed(eq("client1"), eq(1000)))
                .thenReturn(CompletableFuture.completedFuture(true));

        mockMvc.perform(get("/api/check")
                        .param("clientId", "client1")
                        .param("limit", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limit").value(1000));

        verify(rateLimiter).isAllowed("client1", 1000);
    }
}

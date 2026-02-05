package com.example.app;

public record MessageRequest(
    int messageCount,
    boolean externalPayload,
    boolean useFlink
) {}

package io.tembo.pgmq;

import java.time.OffsetDateTime;

public record Message(
        Long id,
        Long readCounter,
        OffsetDateTime enqueuedAt,
        OffsetDateTime visibilityTime,
        String jsonMessage) {
}

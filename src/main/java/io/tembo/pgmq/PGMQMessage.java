package io.tembo.pgmq;

import java.time.OffsetDateTime;

public record PGMQMessage(
        Long id,
        Long readCounter,
        OffsetDateTime enqueuedAt,
        OffsetDateTime visibilityTime,
        String jsonMessage) {
}

package io.tembo.pgmq;

import org.springframework.util.StringUtils;

public record PGMQueue(String name) {

    public PGMQueue {
        if (!StringUtils.hasText(name)) {
            throw new PGMQException("Name of the queue must be not null with non-empty characters!");
        }

        // TODO: valid table name
    }
}

package io.tembo.pgmq;

import org.springframework.util.StringUtils;

public record Queue(String name) {

    public Queue {
        if (!StringUtils.hasText(name)) {
            throw new IllegalArgumentException("Name must be not null with non-empty characters");
        }
    }
}

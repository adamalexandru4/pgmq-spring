package io.tembo.pgmq.config;

import io.tembo.pgmq.Queue;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "pgmq")
public class PGMQConfiguration {

    private List<Queue> queues;

    private int visibilityTimeoutSeconds = 30;

    public List<Queue> getQueues() {
        return queues;
    }

    public void setQueues(List<String> queues) {
        this.queues = queues.stream().map(Queue::new).toList();
    }

    public int getVisibilityTimeoutSeconds() {
        return visibilityTimeoutSeconds;
    }

    public void setVisibilityTimeoutSeconds(int visibilityTimeoutSeconds) {
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    }

}

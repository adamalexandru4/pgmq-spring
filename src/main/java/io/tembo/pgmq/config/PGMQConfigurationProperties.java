package io.tembo.pgmq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

@ConfigurationProperties(prefix = "pgmq")
public class PGMQConfigurationProperties {

    private int delay = 0;

    private int visibilityTimeout = 30;

    private boolean checkMessage = true;

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        Assert.isTrue(delay >= 0, "Delay seconds must be equals or greater than zero!");

        this.delay = delay;
    }

    public int getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(int visibilityTimeout) {
        Assert.isTrue(visibilityTimeout >= 0, "Visibility timeout seconds must be equals or greater than zero!");

        this.visibilityTimeout = visibilityTimeout;
    }

    public boolean isCheckMessage() {
        return checkMessage;
    }

    public void setCheckMessage(boolean checkMessage) {
        this.checkMessage = checkMessage;
    }
}

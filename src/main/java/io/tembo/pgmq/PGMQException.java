package io.tembo.pgmq;

public class PGMQException extends RuntimeException {

    public PGMQException(String message) {
        super(message);
    }

    public PGMQException(String message, Throwable cause) {
        super(message, cause);
    }

}

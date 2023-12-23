package io.tembo.pgmq.json;

public interface PGMQJsonProcessor {

    boolean isJson(String json);

    String toJson(Object object);
}

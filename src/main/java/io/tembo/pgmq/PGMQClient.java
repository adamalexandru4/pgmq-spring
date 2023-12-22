package io.tembo.pgmq;

import io.tembo.pgmq.config.PGMQConfiguration;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Component
public class PGMQClient {

    private final JdbcOperations operations;
    private final PGMQConfiguration configuration;

    public PGMQClient(JdbcOperations operations, PGMQConfiguration configuration) {
        Assert.notNull(operations, "JdbcOperations must be not null!");
        Assert.notNull(configuration, "PGMQConfiguration must be not null!");

        this.operations = operations;
        this.configuration = configuration;
    }

    public void enableExtension() {
        operations.execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE");
    }

    public void createQueue(Queue queue) {
        Assert.notNull(queue, "Queue must be not null!");

        operations.execute("SELECT pgmq.create('" + queue.name() + "')");
    }

    public void dropQueue(Queue queue) {
        Assert.notNull(queue, "Queue must be not null!");

        operations.execute("SELECT pgmq.drop_queue('" + queue.name() + "')");
    }

    public long sendWithDelay(Queue queue, String jsonMessage, int delay) {
        Assert.notNull(queue, "Queue must be not null!");

        return operations.queryForObject("select * from pgmq.send(?, ?::JSONB, ?)", (rs, rn) -> rs.getLong(1), queue.name(), jsonMessage, delay);
    }


    public long send(Queue queue, String jsonMessage) {
        return sendWithDelay(queue, jsonMessage, 0);
    }

    public List<Long> sendBatchWithDelay(Queue queue, List<String> jsonMessages, int delay) {
        Assert.notNull(queue, "Queue must be not null!");

        return operations.query("select * from pgmq.send_batch(?, ?::JSONB[], ?)", (rs, rn) -> rs.getLong(1), queue.name(), jsonMessages.toArray(String[]::new), delay);
    }

    public List<Long> sendBatch(Queue queue, List<String> jsonMessages) {
        return sendBatchWithDelay(queue, jsonMessages, 0);
    }

    public Optional<Message> read(Queue queue) {
        return read(queue, configuration.getVisibilityTimeoutSeconds());
    }

    public Optional<Message> read(Queue queue, int visibilityTime) {
        return Optional.ofNullable(DataAccessUtils.singleResult(readBatch(queue, visibilityTime, 1)));
    }

    public List<Message> readBatch(Queue queue, int visibilityTime, int quantity) {
        Assert.notNull(queue, "Queue must be not null!");
        Assert.isTrue(visibilityTime > 0, "Visibility time for read must be positive!");
        Assert.isTrue(quantity > 0, "Number of messages for read must be positive!");

        return operations.query(
                "select * from pgmq.read(?, ?, ?)",
                (rs, rowNum) -> new Message(
                        rs.getLong("msg_id"),
                        rs.getLong("read_ct"),
                        rs.getObject("enqueued_at", OffsetDateTime.class),
                        rs.getObject("vt", OffsetDateTime.class),
                        rs.getString("message")
                ),
                queue.name(), visibilityTime, quantity);
    }

    public List<Message> readBatch(Queue queue, int quantity) {
        return readBatch(queue, configuration.getVisibilityTimeoutSeconds(), quantity);
    }

    public Optional<Message> pop(Queue queue) {
        Assert.notNull(queue, "Queue must be not null!");

        return Optional.ofNullable(
                DataAccessUtils.singleResult(
                        operations.query(
                                "select * from pgmq.pop(?)",
                                (rs, rowNum) -> new Message(
                                        rs.getLong("msg_id"),
                                        rs.getLong("read_ct"),
                                        rs.getObject("enqueued_at", OffsetDateTime.class),
                                        rs.getObject("vt", OffsetDateTime.class),
                                        rs.getString("message")
                                ),
                                queue.name())
                )
        );
    }

    public boolean delete(Queue queue, long messageId) {
        Assert.notNull(queue, "Queue must be not null!");

        Boolean b = operations.queryForObject("select * from pgmq.delete(?, ?)", Boolean.class, queue.name(), messageId);

        if (b == null) {
            throw new IllegalStateException("Error during deletion of message from queue");
        }

        return b;
    }

    public List<Long> deleteBatch(Queue queue, List<Long> messageIds) {
        Assert.notNull(queue, "Queue must be not null!");

        List<Long> messageIdsDeleted = operations.query("select * from pgmq.delete(?, ?)", (rs, rn) -> rs.getLong(1), queue.name(), messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            System.out.println("WARNING - Some messages were not deleted");
        }

        return messageIdsDeleted;
    }

    public boolean archive(Queue queue, long messageId) {
        Assert.notNull(queue, "Queue must be not null!");

        Boolean b = operations.queryForObject("select * from pgmq.archive(?, ?)", Boolean.class, queue.name(), messageId);

        if (b == null) {
            throw new IllegalStateException("Error during archiving message from queue");
        }

        return b;
    }

    public List<Long> archiveBatch(Queue queue, List<Long> messageIds) {
        Assert.notNull(queue, "Queue must be not null!");

        List<Long> messageIdsDeleted = operations.query("select * from pgmq.archive(?, ?)", (rs, rn) -> rs.getLong(1), queue.name(), messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            System.out.println("WARNING - Some messages were not archived");
        }

        return messageIdsDeleted;
    }


}

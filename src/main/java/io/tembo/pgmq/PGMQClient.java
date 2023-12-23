package io.tembo.pgmq;

import io.tembo.pgmq.config.PGMQConfigurationProperties;
import io.tembo.pgmq.json.PGMQJsonProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

public class PGMQClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PGMQClient.class);

    private final JdbcOperations operations;
    private final PGMQConfigurationProperties configuration;
    private final PGMQJsonProcessor jsonProcessor;

    public PGMQClient(JdbcOperations operations, PGMQConfigurationProperties configuration, PGMQJsonProcessor jsonProcessor) {
        Assert.notNull(operations, "JdbcOperations must be not null!");
        Assert.notNull(configuration, "PGMQConfiguration must be not null!");
        Assert.notNull(jsonProcessor, "PGMQJsonProcessor must be not null!");

        this.operations = operations;
        this.configuration = configuration;
        this.jsonProcessor = jsonProcessor;
    }

    public void enableExtension() {
        try {
            operations.execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE");
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to enable 'pgmq' extension", exception);
        }
    }

    public void createQueue(PGMQueue queue) {
        Assert.notNull(queue, "Queue must be not null!");

        try {
            operations.execute("SELECT pgmq.create('" + queue.name() + "')");
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to create queue " + queue.name(), exception);
        }
    }

    public void dropQueue(PGMQueue queue) {
        Assert.notNull(queue, "Queue must be not null!");

        try {
            operations.execute("SELECT pgmq.drop_queue('" + queue.name() + "')");
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to drop queue " + queue.name(), exception);
        }
    }

    public long sendWithDelay(PGMQueue queue, String jsonMessage, int delay) {
        Assert.notNull(queue, "Queue must be not null!");
        Assert.isTrue(delay >= 0, "Delay seconds must be equals or greater than zero!");

        if (configuration.isCheckMessage()) {
            Assert.isTrue(StringUtils.hasText(jsonMessage), "Message should be not empty!");
            Assert.isTrue(jsonProcessor.isJson(jsonMessage), "Message should be in JSON format!");
        }

        Long messageId;
        try {
            messageId = operations.queryForObject("select * from pgmq.send(?, ?::JSONB, ?)", (rs, rn) -> rs.getLong(1), queue.name(), jsonMessage, delay);
        } catch (DataAccessException exception) {
            throw new PGMQException("Failed to send message on queue " + queue.name(), exception);
        }

        return Optional.ofNullable(messageId)
                .orElseThrow(() -> new PGMQException("No message id provided for sent message!"));
    }


    public long send(PGMQueue queue, String jsonMessage) {
        return sendWithDelay(queue, jsonMessage, 0);
    }

    public List<Long> sendBatchWithDelay(PGMQueue queue, List<String> jsonMessages, int delay) {
        Assert.notNull(queue, "Queue must be not null!");
        Assert.isTrue(delay >= 0, "Delay seconds must be equals or greater than zero!");

        if (configuration.isCheckMessage()) {
            Assert.isTrue(jsonMessages.stream().allMatch(StringUtils::hasText), "Messages should be not empty!");
            Assert.isTrue(jsonMessages.stream().allMatch(jsonProcessor::isJson), "Messages should be in JSON format!");
        }

        return operations.query("select * from pgmq.send_batch(?, ?::JSONB[], ?)", (rs, rn) -> rs.getLong(1), queue.name(), jsonMessages.toArray(String[]::new), delay);
    }

    public List<Long> sendBatch(PGMQueue queue, List<String> jsonMessages) {
        return sendBatchWithDelay(queue, jsonMessages, 0);
    }

    public Optional<PGMQMessage> read(PGMQueue queue) {
        return read(queue, configuration.getVisibilityTimeout());
    }

    public Optional<PGMQMessage> read(PGMQueue queue, int visibilityTime) {
        return Optional.ofNullable(DataAccessUtils.singleResult(readBatch(queue, visibilityTime, 1)));
    }

    public List<PGMQMessage> readBatch(PGMQueue queue, int visibilityTime, int quantity) {
        Assert.notNull(queue, "Queue must be not null!");
        Assert.isTrue(visibilityTime > 0, "Visibility time for read must be positive!");
        Assert.isTrue(quantity > 0, "Number of messages for read must be positive!");

        return operations.query(
                "select * from pgmq.read(?, ?, ?)",
                (rs, rowNum) -> new PGMQMessage(
                        rs.getLong("msg_id"),
                        rs.getLong("read_ct"),
                        rs.getObject("enqueued_at", OffsetDateTime.class),
                        rs.getObject("vt", OffsetDateTime.class),
                        rs.getString("message")
                ),
                queue.name(), visibilityTime, quantity);
    }

    public List<PGMQMessage> readBatch(PGMQueue queue, int quantity) {
        return readBatch(queue, configuration.getVisibilityTimeout(), quantity);
    }

    public Optional<PGMQMessage> pop(PGMQueue queue) {
        Assert.notNull(queue, "Queue must be not null!");

        return Optional.ofNullable(
                DataAccessUtils.singleResult(
                        operations.query(
                                "select * from pgmq.pop(?)",
                                (rs, rowNum) -> new PGMQMessage(
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

    public boolean delete(PGMQueue queue, long messageId) {
        Assert.notNull(queue, "Queue must be not null!");

        Boolean b = operations.queryForObject("select * from pgmq.delete(?, ?)", Boolean.class, queue.name(), messageId);

        if (b == null) {
            throw new PGMQException("Error during deletion of message from queue!");
        }

        return b;
    }

    public List<Long> deleteBatch(PGMQueue queue, List<Long> messageIds) {
        Assert.notNull(queue, "Queue must be not null!");

        List<Long> messageIdsDeleted = operations.query("select * from pgmq.delete(?, ?)", (rs, rn) -> rs.getLong(1), queue.name(), messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            LOGGER.warn("Some messages were not deleted!");
        }

        return messageIdsDeleted;
    }

    public boolean archive(PGMQueue queue, long messageId) {
        Assert.notNull(queue, "Queue must be not null!");

        Boolean b = operations.queryForObject("select * from pgmq.archive(?, ?)", Boolean.class, queue.name(), messageId);

        if (b == null) {
            throw new PGMQException("Error during archiving message from queue!");
        }

        return b;
    }

    public List<Long> archiveBatch(PGMQueue queue, List<Long> messageIds) {
        Assert.notNull(queue, "Queue must be not null!");

        List<Long> messageIdsDeleted = operations.query("select * from pgmq.archive(?, ?)", (rs, rn) -> rs.getLong(1), queue.name(), messageIds.toArray(Long[]::new));

        if (messageIdsDeleted.size() != messageIds.size()) {
            LOGGER.warn("Some messages were not archived!");
        }

        return messageIdsDeleted;
    }


}

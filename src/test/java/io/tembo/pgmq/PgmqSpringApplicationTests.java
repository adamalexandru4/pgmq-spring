package io.tembo.pgmq;

import io.tembo.pgmq.config.PGMQConfigurationProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = PGMQApplicationTest.class)
@Testcontainers
class PgmqSpringApplicationTests {

    @Container
    @ServiceConnection
    private static final PostgreSQLContainer<?> postgreSQLContainer =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("quay.io/tembo/pgmq-pg:latest")
                            .asCompatibleSubstituteFor(PostgreSQLContainer.IMAGE))
                    .withUsername("postgres")
                    .withPassword("password");

    @Autowired
    PGMQConfigurationProperties PGMQConfigurationProperties;

    @Autowired
    PGMQClient pgmqClient;

    @Autowired
    PlatformTransactionManager transactionManager;

    @BeforeEach
    public void setup() {
        pgmqClient.enableExtension();
    }

    @Test
    @DisplayName("""
            Failed to send message on an invalid queue
            """)
    void sendMessageFailedToInvalidQueue() {
        PGMQueue queue = new PGMQueue("wrong-queue");

        assertThrows(PGMQException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John\"}"), "Failed to send message on queue wrong");
    }

    @Test
    @DisplayName("""
            Failed to send empty message
            """)
    void emptyMessage() {
        PGMQueue queue = new PGMQueue("empty_message");
        pgmqClient.createQueue(queue);

        assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, ""), "Message should be not empty!");

        pgmqClient.dropQueue(queue);
    }

    @Test
    @DisplayName("""
            Failed to send wrong JSON format message
            """)
    void wrongJsonFormat() {
        PGMQueue queue = new PGMQueue("wrong_json_message");
        pgmqClient.createQueue(queue);

        assertThrows(IllegalArgumentException.class, () -> pgmqClient.send(queue, "{\"customer_name\": \"John}"), "Message should be in JSON format!");

        pgmqClient.dropQueue(queue);
    }

    @Test
    @DisplayName("""
            Sending multiple messages transactional
            """)
    void sendingTransactional() {
        PGMQueue queue = new PGMQueue("transactional_queue");
        pgmqClient.createQueue(queue);

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        try {
            transactionTemplate.executeWithoutResult((tx) -> {
                pgmqClient.send(queue, "{\"customer_name\": \"John1\"}");
                pgmqClient.send(queue, "{\"customer_name\": \"John2\"}");

                throw new RuntimeException("Something wrong happened");
            });
        } catch (Exception e) {
            // we know it happens
        }

        assertTrue(pgmqClient.readBatch(queue, 2).isEmpty());

        pgmqClient.dropQueue(queue);
    }

    @Test
    @DisplayName("""
            Sending batch of messages
            """)
    void sendingBatchOfMessages() {
        PGMQueue queue = new PGMQueue("batch_queue");
        pgmqClient.createQueue(queue);

        List<Long> batchMessages = pgmqClient.sendBatch(queue,
                List.of(
                        "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }",
                        "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"
                )
        );

        List<PGMQMessage> readMessages = pgmqClient.readBatch(queue, 2);
        Assertions.assertEquals(batchMessages.size(), readMessages.size());

        pgmqClient.dropQueue(queue);
    }

    @Test
    @DisplayName("""
            Read message again if not deleted
            """)
    void readMessageWithoutDelete() throws InterruptedException {
        PGMQueue queue = new PGMQueue("without_delete_queue");
        pgmqClient.createQueue(queue);

        long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

        PGMQMessage message = pgmqClient.read(queue, 1).orElseThrow();
        Assertions.assertEquals(messageId, message.id());

        Thread.sleep(Duration.ofSeconds(2).toMillis());

        PGMQMessage sameMessage = pgmqClient.read(queue).orElseThrow();
        Assertions.assertEquals(messageId, sameMessage.id());

        pgmqClient.dropQueue(queue);
    }

    @Test
    @DisplayName("""
            Delete message after processing
            """)
    void readMessageWithDelete() {
        PGMQueue queue = new PGMQueue("delete_queue");
        pgmqClient.createQueue(queue);

        long messageId = pgmqClient.send(queue, "{\"customer_name\": \"John\"}");

        PGMQMessage message = pgmqClient.read(queue, 1).orElseThrow();
        Assertions.assertEquals(messageId, message.id());

        pgmqClient.delete(queue, messageId);

        assertTrue(pgmqClient.read(queue).isEmpty());

        pgmqClient.dropQueue(queue);
    }

}

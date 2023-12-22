package io.tembo.pgmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.config.PGMQConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.stream.IntStream;

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
    PGMQConfiguration PGMQConfiguration;

    @Autowired
    PGMQClient pgmqClient;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void demoTest() throws JsonProcessingException {
        pgmqClient.enableExtension();

        Queue queue = PGMQConfiguration.getQueues().get(0);
        pgmqClient.createQueue(queue);

        record Customer(
                String name,
                List<String> items,
                long quantity
        ) {
        }

        for (int i = 0; i < 5; i++) {

            long msgId = pgmqClient.send(queue, objectMapper.writeValueAsString(new Customer("Customer " + i, IntStream.range(0, 5).mapToObj(String::valueOf).toList(), i)));
            System.out.println("Sent message " + msgId);
        }

        List<Long> batchMessages = pgmqClient.sendBatch(queue, List.of("{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 100 } }", "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 101 } }"));
        batchMessages.forEach(System.out::println);

        Message readMessage1 = pgmqClient.read(queue).orElseThrow();
        System.out.println(readMessage1);

        pgmqClient.delete(queue, readMessage1.id());


        Message readMessage2 = pgmqClient.read(queue).orElseThrow();
        List<Long> toBeDeleted = List.of(readMessage2.id(), 123123L);
        List<Long> idsDeleted = pgmqClient.deleteBatch(queue, toBeDeleted);
        System.out.println(toBeDeleted);
        System.out.println(idsDeleted);

        pgmqClient.dropQueue(queue);

    }

}

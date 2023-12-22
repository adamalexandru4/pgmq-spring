package io.tembo.pgmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PGMQApplicationTest {

    public static void main(String[] args) {
        SpringApplication.from(PGMQApplicationTest::main).run(args);
    }
}

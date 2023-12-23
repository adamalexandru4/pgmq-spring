package io.tembo.pgmq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.PGMQClient;
import io.tembo.pgmq.json.PGMQJsonProcessor;
import io.tembo.pgmq.json.PGMQJsonProcessorJackson;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcOperations;

@AutoConfiguration(after = {
        JacksonAutoConfiguration.class,
        DataSourceAutoConfiguration.class
})
@EnableConfigurationProperties(PGMQConfigurationProperties.class)
public class PGMQAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(PGMQJsonProcessor.class)
    @ConditionalOnBean(ObjectMapper.class)
    public PGMQJsonProcessor pgmqJsonProcessor(ObjectMapper objectMapper) {
        return new PGMQJsonProcessorJackson(objectMapper);
    }

    @Bean
    @ConditionalOnBean(PGMQJsonProcessor.class)
    public PGMQClient pgmqClient(JdbcOperations jdbcOperations,
                                 PGMQConfigurationProperties pgmqConfigurationProperties,
                                 PGMQJsonProcessor pgmqJsonProcessor) {
        return new PGMQClient(jdbcOperations, pgmqConfigurationProperties, pgmqJsonProcessor);
    }

}

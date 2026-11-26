package com.duoc.ms_vetcare_kafka_admin.config;

import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaAdminClientConfig {
    @Bean(destroyMethod = "close")
    public Admin vetcareKafkaAdmin(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.CLIENT_ID_CONFIG, "vetcare-kafka-admin"
        ));
    }
}

package com.duoc.ms_vetcare_kafka_admin.dto;

import java.util.Map;

public record TopicView(
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configurations) {
}

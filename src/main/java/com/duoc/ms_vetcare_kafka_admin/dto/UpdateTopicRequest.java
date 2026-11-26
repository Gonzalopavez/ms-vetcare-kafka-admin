package com.duoc.ms_vetcare_kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.Map;

/** Partitions: solo puede aumentar. Configurations: SET de opciones permitidas. */
public record UpdateTopicRequest(
        @Min(1) @Max(100) Integer partitions,
        Map<String, String> configurations) {
}

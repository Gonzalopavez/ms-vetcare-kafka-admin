package com.duoc.ms_vetcare_kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.Map;

public record CreateTopicRequest(
        @NotBlank
        @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9._-]{0,248}")
        String name,
        @Min(1) @Max(100) int partitions,
        @Min(1) @Max(20) short replicationFactor,
        Map<String, String> configurations) {
}

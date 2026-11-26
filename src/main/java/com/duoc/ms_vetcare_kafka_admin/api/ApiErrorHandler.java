package com.duoc.ms_vetcare_kafka_admin.api;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiErrorHandler {
    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<Map<String, String>> invalidRequest(IllegalArgumentException error) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", error.getMessage()));
    }
}

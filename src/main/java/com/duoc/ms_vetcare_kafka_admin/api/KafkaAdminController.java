package com.duoc.ms_vetcare_kafka_admin.api;

import com.duoc.ms_vetcare_kafka_admin.dto.ClusterView;
import com.duoc.ms_vetcare_kafka_admin.dto.ConsumerGroupView;
import com.duoc.ms_vetcare_kafka_admin.dto.CreateTopicRequest;
import com.duoc.ms_vetcare_kafka_admin.dto.TopicView;
import com.duoc.ms_vetcare_kafka_admin.dto.UpdateTopicRequest;
import com.duoc.ms_vetcare_kafka_admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {
    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/cluster")
    public ClusterView cluster() { return service.cluster(); }

    @GetMapping("/topics")
    public List<TopicView> topics() { return service.listTopics(); }

    @GetMapping("/topics/{name:.+}")
    public TopicView topic(@PathVariable String name) { return service.getTopic(name); }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public TopicView createTopic(@Valid @RequestBody CreateTopicRequest request) {
        return service.createTopic(request);
    }

    @PatchMapping("/topics/{name:.+}")
    public TopicView updateTopic(@PathVariable String name,
            @Valid @RequestBody UpdateTopicRequest request) {
        return service.updateTopic(name, request);
    }

    @DeleteMapping("/topics/{name:.+}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTopic(@PathVariable String name) { service.deleteTopic(name); }

    @GetMapping("/consumer-groups")
    public List<ConsumerGroupView> consumerGroups() { return service.listConsumerGroups(); }
}

package com.duoc.ms_vetcare_kafka_admin.service;

import com.duoc.ms_vetcare_kafka_admin.dto.CreateTopicRequest;
import com.duoc.ms_vetcare_kafka_admin.dto.UpdateTopicRequest;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class KafkaAdminServiceTest {
    private final Admin admin = mock(Admin.class);
    private final KafkaAdminService service = new KafkaAdminService(admin);

    @Test
    void protegeTopicosCriticos() {
        assertThrows(IllegalArgumentException.class,
                () -> service.deleteTopic("consultations.events"));
        assertThrows(IllegalArgumentException.class,
                () -> service.deleteTopic("__consumer_offsets"));
        verifyNoInteractions(admin);
    }

    @Test
    void rechazaConfiguracionesNoPermitidas() {
        assertThrows(IllegalArgumentException.class,
                () -> service.createTopic(new CreateTopicRequest(
                        "vetcare.demo", 3, (short) 3, Map.of("clave.inventada", "x"))));
        verifyNoInteractions(admin);
    }

    @Test
    void rechazaActualizacionVacia() {
        assertThrows(IllegalArgumentException.class,
                () -> service.updateTopic("vetcare.demo", new UpdateTopicRequest(null, Map.of())));
        verifyNoInteractions(admin);
    }
}

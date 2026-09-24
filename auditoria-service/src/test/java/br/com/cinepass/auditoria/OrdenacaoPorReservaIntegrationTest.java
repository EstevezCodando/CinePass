package br.com.cinepass.auditoria;

import br.com.cinepass.auditoria.application.AuditoriaApplicationService;
import br.com.cinepass.auditoria.domain.model.RegistroAuditoria;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Para a MESMA reserva (mesma chave), ReservaCriada -> ReservaPagamentoAprovado ->
 * ReservaConfirmada são processados na ordem em que foram publicados.
 */
// @Testcontainers antes do @SpringBootTest e @DirtiesContext: o contexto Spring
// (produtor/consumidores Kafka) é fechado antes de os containers pararem.
@Testcontainers
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "eureka.client.enabled=false")
class OrdenacaoPorReservaIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired AuditoriaApplicationService service;

    @Test
    void eventosDaMesmaReservaSaoProcessadosNaOrdemDePublicacao() throws Exception {
        UUID reservaId = UUID.randomUUID();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (String tipo : new String[]{"ReservaCriada", "ReservaPagamentoAprovado", "ReservaConfirmada"}) {
                producer.send(new ProducerRecord<>("reserva.eventos", reservaId.toString(), envelope(tipo, reservaId))).get();
            }
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> service.listarPorReserva(reservaId).size() == 3);

        var registros = service.listarPorReserva(reservaId);
        assertThat(registros).extracting(RegistroAuditoria::getEventType)
                .containsExactly("ReservaCriada", "ReservaPagamentoAprovado", "ReservaConfirmada");
        assertThat(registros).extracting(RegistroAuditoria::getOffset).isSorted();
    }

    private String envelope(String tipo, UUID reservaId) {
        return """
                {"eventId":"%s","eventType":"%s","eventVersion":1,"occurredAt":"2026-01-01T10:00:00Z",
                 "reservaId":"%s","correlationId":"teste-ordem","producer":"reserva-service",
                 "data":{"reservaId":"%s","clienteId":"%s","status":"X"}}
                """.formatted(UUID.randomUUID(), tipo, reservaId, reservaId, UUID.randomUUID());
    }
}

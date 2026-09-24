package br.com.cinepass.fidelidade;

import br.com.cinepass.fidelidade.application.FidelidadeApplicationService;
import br.com.cinepass.fidelidade.domain.model.ClienteFidelidade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Reentregar o mesmo ReservaConfirmada não pode somar pontos nem valor duas vezes. */
// @Testcontainers antes do @SpringBootTest e @DirtiesContext: o contexto Spring
// (produtor/consumidores Kafka) é fechado antes de os containers pararem.
@Testcontainers
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "eureka.client.enabled=false")
class IdempotenciaFidelidadeIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired FidelidadeApplicationService service;

    @Test
    void reservaConfirmadaDuplicadaNaoDuplicaPontosNemValor() throws Exception {
        UUID reservaId = UUID.randomUUID();
        UUID clienteId = UUID.randomUUID();
        String evento = """
                {"eventId":"%s","eventType":"ReservaConfirmada","eventVersion":1,"occurredAt":"2026-01-01T10:00:00Z",
                 "reservaId":"%s","correlationId":"teste","producer":"reserva-service",
                 "data":{"reservaId":"%s","clienteId":"%s","assentos":["A1","A2"],"valorTotal":79.80,"status":"CONFIRMADA"}}
                """.formatted(UUID.randomUUID(), reservaId, reservaId, clienteId);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < 2; i++) { // mesma mensagem (mesmo eventId) entregue duas vezes
                var record = new ProducerRecord<>("reserva.eventos", reservaId.toString(), evento);
                record.headers().add(new RecordHeader("eventType", "ReservaConfirmada".getBytes(StandardCharsets.UTF_8)));
                producer.send(record).get();
            }
        }

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> service.listar().stream().anyMatch(c -> c.getClienteId().equals(clienteId)));
        Thread.sleep(3000);

        ClienteFidelidade cliente = service.buscar(clienteId);
        assertThat(cliente.getReservasConfirmadas()).isEqualTo(1);
        assertThat(cliente.getIngressosComprados()).isEqualTo(2);
        assertThat(cliente.getValorTotalGasto()).isEqualByComparingTo(new BigDecimal("79.80"));
        assertThat(cliente.getPontos()).isEqualTo(79);
    }
}

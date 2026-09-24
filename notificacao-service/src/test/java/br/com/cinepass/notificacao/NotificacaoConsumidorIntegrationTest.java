package br.com.cinepass.notificacao;

import br.com.cinepass.notificacao.application.NotificacaoApplicationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

// @Testcontainers antes do @SpringBootTest e @DirtiesContext: o contexto Spring
// (produtor/consumidores Kafka) é fechado antes de os containers pararem.
@Testcontainers
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "eureka.client.enabled=false")
class NotificacaoConsumidorIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired NotificacaoApplicationService service;

    @Test
    void mensagemDuplicadaNaoGeraSegundaNotificacao() throws Exception {
        UUID reservaId = UUID.randomUUID();
        String evento = envelope(UUID.randomUUID(), "ReservaConfirmada", reservaId);

        publicar(reservaId.toString(), evento);
        publicar(reservaId.toString(), evento); // reentrega do MESMO eventId

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> service.listarPorReserva(reservaId).size() == 1);
        Thread.sleep(3000);
        assertThat(service.listarPorReserva(reservaId)).hasSize(1);
    }

    @Test
    void mensagemInvalidaVaiParaDltSemBloquearAsDemais() throws Exception {
        String chaveInvalida = "mensagem-invalida-" + UUID.randomUUID();
        UUID reservaValida = UUID.randomUUID();

        publicar(chaveInvalida, "{ isto nao e um evento");
        publicar(reservaValida.toString(), envelope(UUID.randomUUID(), "ReservaCriada", reservaValida));

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> service.listarPorReserva(reservaValida).size() == 1);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("reserva.eventos.notificacao.dlt"));
            ConsumerRecord<String, String> morta = KafkaTestUtils.getSingleRecord(consumer, "reserva.eventos.notificacao.dlt", Duration.ofSeconds(30));
            assertThat(morta.key()).isEqualTo(chaveInvalida);
            assertThat(morta.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE)).isNotNull();
            assertThat(morta.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
        }
    }

    private void publicar(String chave, String valor) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>("reserva.eventos", chave, valor)).get();
        }
    }

    private String envelope(UUID eventId, String tipo, UUID reservaId) {
        return """
                {"eventId":"%s","eventType":"%s","eventVersion":1,"occurredAt":"2026-01-01T10:00:00Z",
                 "reservaId":"%s","correlationId":"teste","producer":"reserva-service",
                 "data":{"reservaId":"%s","clienteId":"%s","sessaoId":"%s","assentos":["A1"],"valorTotal":39.90,"status":"X"}}
                """.formatted(eventId, tipo, reservaId, reservaId, UUID.randomUUID(), UUID.randomUUID());
    }
}

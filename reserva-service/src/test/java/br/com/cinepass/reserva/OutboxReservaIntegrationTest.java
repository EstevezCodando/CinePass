package br.com.cinepass.reserva;

import br.com.cinepass.reserva.application.FalhaIntegracaoException;
import br.com.cinepass.reserva.application.FalhaProcessamentoReservaException;
import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaApplicationService;
import br.com.cinepass.reserva.application.port.IngressoGateway;
import br.com.cinepass.reserva.application.port.PagamentoGateway;
import io.temporal.client.WorkflowClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Publicação transacional com PostgreSQL e Kafka reais (Testcontainers).
 * Pagamento e ingresso são simulados: o foco aqui é o reserva-service.
 */
// @Testcontainers antes do @SpringBootTest e @DirtiesContext: o contexto Spring
// (produtor/consumidores Kafka) é fechado antes de os containers pararem.
@Testcontainers
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "eureka.client.enabled=false",
        "cinepass.outbox.poll-interval-ms=200",
        "spring.autoconfigure.exclude="
                + "io.temporal.spring.boot.autoconfigure.ServiceStubsAutoConfiguration,"
                + "io.temporal.spring.boot.autoconfigure.RootNamespaceAutoConfiguration,"
                + "io.temporal.spring.boot.autoconfigure.NonRootNamespaceAutoConfiguration,"
                + "io.temporal.spring.boot.autoconfigure.TestServerAutoConfiguration"
})
class OutboxReservaIntegrationTest {

    private static final UUID SESSAO = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @MockitoBean WorkflowClient workflowClient;
    @MockitoBean PagamentoGateway pagamentoGateway;
    @MockitoBean IngressoGateway ingressoGateway;

    @Autowired ReservaApplicationService service;
    @Autowired JdbcTemplate jdbc;

    @Test
    void eventosDaReservaSaoPublicadosEmOrdemNaMesmaParticaoComChaveReservaId() {
        when(pagamentoGateway.cobrar(any(), any(), anyBoolean()))
                .thenReturn(new PagamentoGateway.PagamentoResultado(UUID.randomUUID(), "APROVADO"));
        when(ingressoGateway.emitir(any(), any(), anyList(), anyBoolean()))
                .thenReturn(new IngressoGateway.IngressoResultado(UUID.randomUUID(), "CINE-TESTE", "EMITIDO"));

        var reserva = service.realizar(new RealizarReservaCommand(UUID.randomUUID(), SESSAO, List.of("A1"), false, false));

        List<ConsumerRecord<String, String>> recebidos = consumirAte(3, reserva.reservaId().toString());
        assertThat(recebidos).extracting(r -> new String(r.headers().lastHeader("eventType").value(), StandardCharsets.UTF_8))
                .containsExactly("ReservaCriada", "ReservaPagamentoAprovado", "ReservaConfirmada");
        assertThat(recebidos).extracting(ConsumerRecord::partition).containsOnly(recebidos.get(0).partition());
        assertThat(recebidos.get(0).offset()).isLessThan(recebidos.get(1).offset());
        assertThat(recebidos.get(1).offset()).isLessThan(recebidos.get(2).offset());
    }

    @Test
    void quandoAReservaFalhaORollbackNaoDeixaEventoNaOutbox() {
        when(pagamentoGateway.cobrar(any(), any(), anyBoolean()))
                .thenReturn(new PagamentoGateway.PagamentoResultado(UUID.randomUUID(), "APROVADO"));
        when(ingressoGateway.emitir(any(), any(), anyList(), anyBoolean()))
                .thenThrow(new FalhaIntegracaoException("ingresso indisponível", null));

        assertThatThrownBy(() -> service.realizar(new RealizarReservaCommand(UUID.randomUUID(), SESSAO, List.of("B1"), false, true)))
                .isInstanceOf(FalhaProcessamentoReservaException.class)
                .satisfies(ex -> {
                    UUID reservaId = ((FalhaProcessamentoReservaException) ex).getReservaId();
                    Integer eventos = jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?", Integer.class, reservaId);
                    assertThat(eventos).isZero();
                });
    }

    private List<ConsumerRecord<String, String>> consumirAte(int quantidade, String chave) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> encontrados = new ArrayList<>();
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("reserva.eventos"));
            long limite = System.currentTimeMillis() + 30_000;
            while (encontrados.size() < quantidade && System.currentTimeMillis() < limite) {
                consumer.poll(Duration.ofSeconds(2)).forEach(r -> {
                    if (chave.equals(r.key())) encontrados.add(r);
                });
            }
        }
        return encontrados;
    }
}

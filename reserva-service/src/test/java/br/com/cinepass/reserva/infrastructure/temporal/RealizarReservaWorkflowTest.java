package br.com.cinepass.reserva.infrastructure.temporal;

import br.com.cinepass.reserva.application.PagamentoRecusadoException;
import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaCompensadaException;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import br.com.cinepass.reserva.infrastructure.temporal.activity.IngressoActivity;
import br.com.cinepass.reserva.infrastructure.temporal.activity.PagamentoActivity;
import br.com.cinepass.reserva.infrastructure.temporal.activity.ReservaActivity;
import br.com.cinepass.reserva.infrastructure.temporal.workflow.RealizarReservaWorkflowImpl;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Saga orquestrada pelo Temporal (servidor de teste em memória): tipos de falha
 * devolvidos ao orquestrador, compensação e propagação do correlationId até as
 * activities.
 */
class RealizarReservaWorkflowTest {

    private static final UUID SESSAO = UUID.fromString("22222222-2222-2222-2222-222222222222");

    private TestWorkflowEnvironment ambiente;
    private final List<String> chamadas = Collections.synchronizedList(new ArrayList<>());
    private final List<String> correlationIds = Collections.synchronizedList(new ArrayList<>());
    private TemporalReservaOrquestrador orquestrador;

    @BeforeEach
    void iniciar() {
        ambiente = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder()
                .setWorkflowClientOptions(WorkflowClientOptions.newBuilder()
                        .setContextPropagators(List.of(new CorrelationIdContextPropagator()))
                        .build())
                .build());
        Worker worker = ambiente.newWorker("cinepass-reserva");
        worker.registerWorkflowImplementationTypes(RealizarReservaWorkflowImpl.class);
        worker.registerActivitiesImplementations(new ReservaFake(), new PagamentoFake(), new IngressoFake());
        ambiente.start();
        orquestrador = new TemporalReservaOrquestrador(ambiente.getWorkflowClient());
    }

    @AfterEach
    void encerrar() {
        MDC.clear();
        ambiente.close();
    }

    @Test
    void caminhoFelizConfirmaAReservaEPropagaOCorrelationIdParaTodasAsActivities() {
        MDC.put("correlationId", "teste-temporal-001");

        ReservaDetalhe reserva = orquestrador.realizar(comando(false, false));

        assertThat(reserva.status()).isEqualTo("CONFIRMADA");
        assertThat(chamadas).containsExactly("iniciar", "cobrar", "confirmarPagamento", "iniciarEmissaoIngresso", "emitir", "confirmar");
        assertThat(correlationIds).hasSize(chamadas.size()).containsOnly("teste-temporal-001");
    }

    @Test
    void pagamentoRecusadoCancelaAReservaEViraPagamentoRecusadoException() {
        MDC.put("correlationId", "teste-temporal-002");

        assertThatThrownBy(() -> orquestrador.realizar(comando(true, false)))
                .isInstanceOf(PagamentoRecusadoException.class);
        assertThat(chamadas).containsExactly("iniciar", "cobrar", "cancelar");
        assertThat(correlationIds).containsOnly("teste-temporal-002");
    }

    @Test
    void falhaNaEmissaoDoIngressoCompensaEViraReservaCompensadaException() {
        MDC.put("correlationId", "teste-temporal-003");

        assertThatThrownBy(() -> orquestrador.realizar(comando(false, true)))
                .isInstanceOf(ReservaCompensadaException.class)
                .satisfies(ex -> {
                    ReservaCompensadaException compensada = (ReservaCompensadaException) ex;
                    assertThat(compensada.getReservaId()).isNotNull();
                    assertThat(compensada.getPagamentoId()).isNotNull();
                });
        assertThat(chamadas).containsExactly("iniciar", "cobrar", "confirmarPagamento", "iniciarEmissaoIngresso",
                "emitir", "estornar", "cancelar");
        assertThat(correlationIds).containsOnly("teste-temporal-003");
    }

    private static RealizarReservaCommand comando(boolean recusarPagamento, boolean falharIngresso) {
        return new RealizarReservaCommand(UUID.randomUUID(), SESSAO, List.of("A1"), recusarPagamento, falharIngresso);
    }

    private void registrar(String chamada) {
        chamadas.add(chamada);
        correlationIds.add(String.valueOf(MDC.get("correlationId")));
    }

    private static ReservaDetalhe detalhe(UUID reservaId, String status) {
        return new ReservaDetalhe(reservaId, UUID.randomUUID(), SESSAO, List.of("A1"), new BigDecimal("39.90"),
                status, null, null, Instant.now());
    }

    private class ReservaFake implements ReservaActivity {
        public ReservaDetalhe iniciar(RealizarReservaCommand command) { registrar("iniciar"); return detalhe(UUID.randomUUID(), "AGUARDANDO_PAGAMENTO"); }
        public ReservaDetalhe confirmarPagamento(UUID reservaId, UUID pagamentoId) { registrar("confirmarPagamento"); return detalhe(reservaId, "PAGAMENTO_APROVADO"); }
        public ReservaDetalhe cancelar(UUID reservaId) { registrar("cancelar"); return detalhe(reservaId, "CANCELADA"); }
        public ReservaDetalhe confirmar(UUID reservaId, UUID ingressoId) { registrar("confirmar"); return detalhe(reservaId, "CONFIRMADA"); }
        public ReservaDetalhe iniciarEmissaoIngresso(UUID reservaId) { registrar("iniciarEmissaoIngresso"); return detalhe(reservaId, "EMITINDO_INGRESSO"); }
    }

    private class PagamentoFake implements PagamentoActivity {
        public Resultado cobrar(UUID reservaId, UUID clienteId, BigDecimal valor, boolean simularRecusa) {
            registrar("cobrar");
            return new Resultado(UUID.randomUUID(), simularRecusa ? "RECUSADO" : "APROVADO");
        }
        public Resultado estornar(UUID pagamentoId) { registrar("estornar"); return new Resultado(pagamentoId, "ESTORNADO"); }
    }

    private class IngressoFake implements IngressoActivity {
        public Resultado emitir(UUID reservaId, UUID sessaoId, List<String> assentos, boolean simularFalha) {
            registrar("emitir");
            if (simularFalha) {
                throw new IllegalStateException("falha simulada na emissão do ingresso");
            }
            return new Resultado(UUID.randomUUID(), "CINE-TESTE", "EMITIDO");
        }
    }
}

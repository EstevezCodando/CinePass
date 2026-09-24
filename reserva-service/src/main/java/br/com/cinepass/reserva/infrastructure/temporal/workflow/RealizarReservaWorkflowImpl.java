package br.com.cinepass.reserva.infrastructure.temporal.workflow;

import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import br.com.cinepass.reserva.infrastructure.temporal.activity.IngressoActivity;
import br.com.cinepass.reserva.infrastructure.temporal.activity.PagamentoActivity;
import br.com.cinepass.reserva.infrastructure.temporal.activity.ReservaActivity;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;

import java.time.Duration;

@WorkflowImpl(taskQueues = "cinepass-reserva")
public class RealizarReservaWorkflowImpl implements RealizarReservaWorkflow {

    /** Tipos das falhas de negócio devolvidas ao TemporalReservaOrquestrador. */
    public static final String TIPO_PAGAMENTO_RECUSADO = "PAGAMENTO_RECUSADO";
    public static final String TIPO_FALHA_EMISSAO_INGRESSO = "FALHA_EMISSAO_INGRESSO";

    private final ActivityOptions options =
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(10))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setMaximumAttempts(1)
                            .build())
                    .build();
    private final ReservaActivity reservaActivity =
            Workflow.newActivityStub(ReservaActivity.class,options);


    private final PagamentoActivity pagamentoActivity =
            Workflow.newActivityStub(
                    PagamentoActivity.class,
                    options
            );
    private final IngressoActivity ingressoActivity =
            Workflow.newActivityStub(
                    IngressoActivity.class,
                    options
            );

    @Override
    public ReservaDetalhe realizar(RealizarReservaCommand command) {
        ReservaDetalhe reserva = reservaActivity.iniciar(command);

        PagamentoActivity.Resultado pagamento = pagamentoActivity.cobrar(reserva.reservaId(),
                reserva.clienteId(), reserva.valorTotal(), command.simularRecusaPagamento());

        if(!"APROVADO".equals(pagamento.status())){
            reservaActivity.cancelar(reserva.reservaId());
            throw ApplicationFailure.newNonRetryableFailure("Pagamento recusado", TIPO_PAGAMENTO_RECUSADO,
                    reserva.reservaId().toString());
        }
        reserva = reservaActivity.confirmarPagamento(reserva.reservaId(),pagamento.pagamentoId());
        reserva = reservaActivity.iniciarEmissaoIngresso(reserva.reservaId());
        try {
            IngressoActivity.Resultado ingresso = ingressoActivity.emitir(reserva.reservaId(),
                    reserva.sessaoId(),
                    reserva.assentos(),
                    command.simularFalhaIngresso());
            return reservaActivity.confirmar(reserva.reservaId(),ingresso.ingressoId());
        }catch (ActivityFailure ex){
            // compensação: estorna o pagamento e cancela a reserva (libera os assentos)
            pagamentoActivity.estornar(pagamento.pagamentoId());
            reservaActivity.cancelar(reserva.reservaId());
            throw ApplicationFailure.newNonRetryableFailure(
                    "Falha na emissão do ingresso: pagamento estornado e reserva cancelada",
                    TIPO_FALHA_EMISSAO_INGRESSO,
                    reserva.reservaId().toString(), pagamento.pagamentoId().toString());
        }
    }
}

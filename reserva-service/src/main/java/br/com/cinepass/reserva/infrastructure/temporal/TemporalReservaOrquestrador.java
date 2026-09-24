package br.com.cinepass.reserva.infrastructure.temporal;

import br.com.cinepass.reserva.application.PagamentoRecusadoException;
import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaCompensadaException;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import br.com.cinepass.reserva.application.port.ReservaOrquestrador;
import br.com.cinepass.reserva.infrastructure.temporal.workflow.RealizarReservaWorkflow;
import br.com.cinepass.reserva.infrastructure.temporal.workflow.RealizarReservaWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.failure.ApplicationFailure;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class TemporalReservaOrquestrador implements ReservaOrquestrador {
    private final WorkflowClient workflowClient;

    public TemporalReservaOrquestrador(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    @Override
    public ReservaDetalhe realizar(RealizarReservaCommand command) {
        RealizarReservaWorkflow workflow = workflowClient
                .newWorkflowStub(RealizarReservaWorkflow.class,
                        WorkflowOptions.newBuilder()
                                .setTaskQueue("cinepass-reserva")
                                .setWorkflowId("reserva-" + UUID.randomUUID())
                                .build()
                );
        try {
            return workflow.realizar(command);
        }catch (WorkflowFailedException ex){
            if(ex.getCause() instanceof ApplicationFailure failure){
                if(RealizarReservaWorkflowImpl.TIPO_PAGAMENTO_RECUSADO.equals(failure.getType())){
                    throw new PagamentoRecusadoException(detalhe(failure, 0));
                }
                if(RealizarReservaWorkflowImpl.TIPO_FALHA_EMISSAO_INGRESSO.equals(failure.getType())){
                    throw new ReservaCompensadaException(failure.getOriginalMessage(), detalhe(failure, 0), detalhe(failure, 1));
                }
            }
            throw ex;
        }
    }

    private static UUID detalhe(ApplicationFailure failure, int indice) {
        return UUID.fromString(failure.getDetails().get(indice, String.class));
    }
}

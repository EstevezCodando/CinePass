package br.com.cinepass.reserva.infrastructure.temporal;

import br.com.cinepass.reserva.application.exception.PagamentoRecusadoException;
import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import br.com.cinepass.reserva.application.port.ReservaOrquestrador;
import br.com.cinepass.reserva.infrastructure.temporal.workflow.RealizarReservaWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.Workflow;
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
            if(ex.getCause() instanceof ApplicationFailure failure && "PAGAMENTO_RECUSADO".equals(failure.getType())){
                throw new PagamentoRecusadoException();
            }
            throw ex;
        }
    }
}

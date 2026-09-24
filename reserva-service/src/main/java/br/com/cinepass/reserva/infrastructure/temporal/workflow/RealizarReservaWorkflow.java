package br.com.cinepass.reserva.infrastructure.temporal.workflow;

import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface RealizarReservaWorkflow {
    @WorkflowMethod
    ReservaDetalhe realizar(RealizarReservaCommand command);
}

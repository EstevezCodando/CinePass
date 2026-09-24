package br.com.cinepass.reserva.infrastructure.temporal.activity;

import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.UUID;

@ActivityInterface
public interface ReservaActivity {
    @ActivityMethod
    ReservaDetalhe iniciar(RealizarReservaCommand command);
    @ActivityMethod
    ReservaDetalhe confirmarPagamento(UUID reservaId, UUID pagamentoId);
    @ActivityMethod
    ReservaDetalhe cancelar(UUID reservaId);
    @ActivityMethod
    ReservaDetalhe confirmar(
            UUID reservaId,
            UUID ingressoId
    );
    @ActivityMethod
    ReservaDetalhe iniciarEmissaoIngresso(
            UUID reservaId
    );
}

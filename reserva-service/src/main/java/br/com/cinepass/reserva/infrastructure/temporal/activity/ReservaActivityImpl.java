package br.com.cinepass.reserva.infrastructure.temporal.activity;

import br.com.cinepass.reserva.application.RealizarReservaCommand;
import br.com.cinepass.reserva.application.ReservaApplicationService;
import br.com.cinepass.reserva.application.ReservaDetalhe;
import io.temporal.spring.boot.ActivityImpl;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@ActivityImpl(taskQueues = "cinepass-reserva")
public class ReservaActivityImpl implements ReservaActivity {

    private final ReservaApplicationService reservaService;

    public ReservaActivityImpl(ReservaApplicationService reservaService) {
        this.reservaService = reservaService;
    }

    @Override
    public ReservaDetalhe iniciar(RealizarReservaCommand command) {
        return reservaService.iniciar(command);
    }

    @Override
    public ReservaDetalhe confirmarPagamento(UUID reservaId, UUID pagamentoId) {
        return reservaService.confirmarPagamento(reservaId,pagamentoId);
    }

    @Override
    public ReservaDetalhe cancelar(UUID reservaId) {
        return reservaService.cancelar(reservaId);
    }

    @Override
    public ReservaDetalhe confirmar(UUID reservaId, UUID ingressoId) {
        return reservaService.confirmar(reservaId,ingressoId);
    }

    @Override
    public ReservaDetalhe iniciarEmissaoIngresso(UUID reservaId) {
        return reservaService.iniciarEmissaoIngresso(reservaId);
    }
}

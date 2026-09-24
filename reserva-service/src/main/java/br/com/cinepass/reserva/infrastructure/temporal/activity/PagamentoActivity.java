package br.com.cinepass.reserva.infrastructure.temporal.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.math.BigDecimal;
import java.util.UUID;

@ActivityInterface
public interface PagamentoActivity {
    @ActivityMethod
    Resultado cobrar(UUID reservaId, UUID clienteId, BigDecimal valor, boolean simularRecusa);
    @ActivityMethod
    Resultado estornar(
            UUID pagamentoId
    );
    record Resultado (UUID pagamentoId, String status){}
}

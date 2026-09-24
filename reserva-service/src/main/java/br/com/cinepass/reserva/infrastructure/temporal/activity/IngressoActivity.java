package br.com.cinepass.reserva.infrastructure.temporal.activity;

import io.temporal.activity.ActivityInterface;

import java.util.List;
import java.util.UUID;

@ActivityInterface
public interface IngressoActivity {
    Resultado emitir(UUID reservaId, UUID sesssaoId, List<String> assentos, boolean simularFalha);
    record Resultado(
            UUID ingressoId,
            String codigo,
            String status
    ) {}
}

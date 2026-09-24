package br.com.cinepass.reserva.domain.event;

import br.com.cinepass.reserva.domain.model.AssentoId;
import br.com.cinepass.reserva.domain.model.Reserva;
import br.com.cinepass.reserva.domain.shared.DomainEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ReservaConfirmada(UUID eventId, Instant occurredAt, UUID reservaId, UUID clienteId, UUID sessaoId,
                                List<String> assentos, BigDecimal valorTotal, UUID pagamentoId, UUID ingressoId)
        implements DomainEvent {
    public static ReservaConfirmada de(Reserva r) {
        return new ReservaConfirmada(UUID.randomUUID(), Instant.now(), r.getId().valor(), r.getClienteId().valor(),
                r.getSessaoId().valor(), r.getAssentos().stream().map(AssentoId::valor).toList(), r.getValorTotal().valor(),
                r.getPagamentoId(), r.getIngressoId());
    }

    @Override
    public String eventType() { return "ReservaConfirmada"; }
}

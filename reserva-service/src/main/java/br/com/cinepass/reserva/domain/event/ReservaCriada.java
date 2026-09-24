package br.com.cinepass.reserva.domain.event;

import br.com.cinepass.reserva.domain.model.AssentoId;
import br.com.cinepass.reserva.domain.model.Reserva;
import br.com.cinepass.reserva.domain.shared.DomainEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ReservaCriada(UUID eventId, Instant occurredAt, UUID reservaId, UUID clienteId, UUID sessaoId,
                            List<String> assentos, BigDecimal valorTotal) implements DomainEvent {
    public static ReservaCriada de(Reserva r) {
        return new ReservaCriada(UUID.randomUUID(), Instant.now(), r.getId().valor(), r.getClienteId().valor(),
                r.getSessaoId().valor(), r.getAssentos().stream().map(AssentoId::valor).toList(), r.getValorTotal().valor());
    }

    @Override
    public String eventType() { return "ReservaCriada"; }
}

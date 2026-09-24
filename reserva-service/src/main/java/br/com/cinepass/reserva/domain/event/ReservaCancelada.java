package br.com.cinepass.reserva.domain.event;

import br.com.cinepass.reserva.domain.model.AssentoId;
import br.com.cinepass.reserva.domain.model.Reserva;
import br.com.cinepass.reserva.domain.shared.DomainEvent;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ReservaCancelada(UUID eventId, Instant occurredAt, UUID reservaId, UUID clienteId, UUID sessaoId,
                               List<String> assentos, String statusAnterior) implements DomainEvent {
    public static ReservaCancelada de(Reserva r, String statusAnterior) {
        return new ReservaCancelada(UUID.randomUUID(), Instant.now(), r.getId().valor(), r.getClienteId().valor(),
                r.getSessaoId().valor(), r.getAssentos().stream().map(AssentoId::valor).toList(), statusAnterior);
    }

    @Override
    public String eventType() { return "ReservaCancelada"; }
}

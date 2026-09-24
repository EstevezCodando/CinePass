package br.com.cinepass.reserva.domain.event;

import br.com.cinepass.reserva.domain.model.Reserva;
import br.com.cinepass.reserva.domain.shared.DomainEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ReservaPagamentoAprovado(UUID eventId, Instant occurredAt, UUID reservaId, UUID clienteId,
                                       UUID pagamentoId, BigDecimal valorTotal) implements DomainEvent {
    public static ReservaPagamentoAprovado de(Reserva r) {
        return new ReservaPagamentoAprovado(UUID.randomUUID(), Instant.now(), r.getId().valor(), r.getClienteId().valor(),
                r.getPagamentoId(), r.getValorTotal().valor());
    }

    @Override
    public String eventType() { return "ReservaPagamentoAprovado"; }
}

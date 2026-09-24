package br.com.cinepass.reserva.infrastructure.messaging;

import br.com.cinepass.reserva.domain.event.ReservaCancelada;
import br.com.cinepass.reserva.domain.event.ReservaConfirmada;
import br.com.cinepass.reserva.domain.event.ReservaCriada;
import br.com.cinepass.reserva.domain.event.ReservaPagamentoAprovado;
import br.com.cinepass.reserva.domain.shared.DomainEvent;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Converte um evento de domínio do Aggregate Reserva no envelope publicado no Kafka. */
@Component
public class DomainEventMapper {

    private static final String PRODUCER = "reserva-service";
    private static final int EVENT_VERSION = 1;

    public ReservaEventEnvelope paraEnvelope(DomainEvent event, UUID reservaId, String correlationId) {
        return new ReservaEventEnvelope(event.eventId(), event.eventType(), EVENT_VERSION, event.occurredAt(),
                reservaId, correlationId, PRODUCER, paraDados(event));
    }

    private Map<String, Object> paraDados(DomainEvent event) {
        Map<String, Object> data = new LinkedHashMap<>();
        switch (event) {
            case ReservaCriada e -> {
                data.put("reservaId", e.reservaId());
                data.put("clienteId", e.clienteId());
                data.put("sessaoId", e.sessaoId());
                data.put("assentos", e.assentos());
                data.put("valorTotal", e.valorTotal());
                data.put("status", "AGUARDANDO_PAGAMENTO");
            }
            case ReservaPagamentoAprovado e -> {
                data.put("reservaId", e.reservaId());
                data.put("clienteId", e.clienteId());
                data.put("pagamentoId", e.pagamentoId());
                data.put("valorTotal", e.valorTotal());
                data.put("status", "PAGAMENTO_APROVADO");
            }
            case ReservaConfirmada e -> {
                data.put("reservaId", e.reservaId());
                data.put("clienteId", e.clienteId());
                data.put("sessaoId", e.sessaoId());
                data.put("assentos", e.assentos());
                data.put("valorTotal", e.valorTotal());
                data.put("pagamentoId", e.pagamentoId());
                data.put("ingressoId", e.ingressoId());
                data.put("status", "CONFIRMADA");
            }
            case ReservaCancelada e -> {
                data.put("reservaId", e.reservaId());
                data.put("clienteId", e.clienteId());
                data.put("sessaoId", e.sessaoId());
                data.put("assentos", e.assentos());
                data.put("statusAnterior", e.statusAnterior());
                data.put("status", "CANCELADA");
            }
            default -> throw new IllegalArgumentException("Evento sem mapeamento de payload: " + event.eventType());
        }
        return data;
    }
}

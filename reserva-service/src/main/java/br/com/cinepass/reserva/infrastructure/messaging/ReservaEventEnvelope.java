package br.com.cinepass.reserva.infrastructure.messaging;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Envelope comum de todos os eventos publicados pelo reserva-service no tópico
 * {@code reserva.eventos}. Estrutura documentada em {@code docs/EVENTS.md}.
 */
public record ReservaEventEnvelope(
        UUID eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        UUID reservaId,
        String correlationId,
        String producer,
        Map<String, Object> data) {
}

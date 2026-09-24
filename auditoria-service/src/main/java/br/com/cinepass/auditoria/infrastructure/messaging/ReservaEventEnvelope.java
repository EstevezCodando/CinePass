package br.com.cinepass.auditoria.infrastructure.messaging;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Espelha o envelope publicado pelo reserva-service no tópico reserva.eventos (ver docs/EVENTS.md). */
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

package br.com.cinepass.auditoria.application;

import java.time.Instant;
import java.util.UUID;

public record EventoParaAuditoria(UUID eventId, UUID reservaId, String eventType, String correlationId, String payload,
                                  Instant occurredAt, int particao, long offset) {
}

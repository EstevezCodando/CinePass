package br.com.cinepass.auditoria.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Registro de um evento processado pela plataforma, com o conteúdo original da mensagem. */
public class RegistroAuditoria {
    private final UUID id;
    private final UUID eventId;
    private final UUID reservaId;
    private final String eventType;
    private final String correlationId;
    private final String payload;
    private final Instant occurredAt;
    private final Instant recebidoEm;
    private final int particao;
    private final long offset;

    private RegistroAuditoria(UUID id, UUID eventId, UUID reservaId, String eventType, String correlationId, String payload,
                              Instant occurredAt, Instant recebidoEm, int particao, long offset) {
        this.id = Objects.requireNonNull(id);
        this.eventId = Objects.requireNonNull(eventId, "O eventId é obrigatório.");
        this.reservaId = Objects.requireNonNull(reservaId);
        this.eventType = Objects.requireNonNull(eventType);
        this.correlationId = correlationId;
        this.payload = Objects.requireNonNull(payload);
        this.occurredAt = occurredAt;
        this.recebidoEm = Objects.requireNonNull(recebidoEm);
        this.particao = particao;
        this.offset = offset;
    }

    public static RegistroAuditoria registrar(UUID eventId, UUID reservaId, String eventType, String correlationId,
                                              String payload, Instant occurredAt, int particao, long offset) {
        return new RegistroAuditoria(UUID.randomUUID(), eventId, reservaId, eventType, correlationId, payload,
                occurredAt, Instant.now(), particao, offset);
    }

    public static RegistroAuditoria restaurar(UUID id, UUID eventId, UUID reservaId, String eventType, String correlationId,
                                              String payload, Instant occurredAt, Instant recebidoEm, int particao, long offset) {
        return new RegistroAuditoria(id, eventId, reservaId, eventType, correlationId, payload, occurredAt, recebidoEm, particao, offset);
    }

    public UUID getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getReservaId() { return reservaId; }
    public String getEventType() { return eventType; }
    public String getCorrelationId() { return correlationId; }
    public String getPayload() { return payload; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getRecebidoEm() { return recebidoEm; }
    public int getParticao() { return particao; }
    public long getOffset() { return offset; }
}

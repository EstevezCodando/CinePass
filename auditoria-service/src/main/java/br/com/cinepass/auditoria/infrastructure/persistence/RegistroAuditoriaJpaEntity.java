package br.com.cinepass.auditoria.infrastructure.persistence;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "auditoria_eventos", indexes = @Index(name = "idx_auditoria_reserva", columnList = "reserva_id"))
public class RegistroAuditoriaJpaEntity {
    @Id private UUID id;
    @Column(name = "event_id", nullable = false, unique = true) private UUID eventId;
    @Column(name = "reserva_id", nullable = false) private UUID reservaId;
    @Column(name = "event_type", nullable = false, length = 60) private String eventType;
    @Column(name = "correlation_id", length = 80) private String correlationId;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Column(name = "occurred_at") private Instant occurredAt;
    @Column(name = "recebido_em", nullable = false) private Instant recebidoEm;
    @Column(nullable = false) private int particao;
    @Column(name = "offset_kafka", nullable = false) private long offsetKafka;

    protected RegistroAuditoriaJpaEntity() {}

    public RegistroAuditoriaJpaEntity(UUID id, UUID eventId, UUID reservaId, String eventType, String correlationId, String payload,
                                      Instant occurredAt, Instant recebidoEm, int particao, long offsetKafka) {
        this.id = id; this.eventId = eventId; this.reservaId = reservaId; this.eventType = eventType;
        this.correlationId = correlationId; this.payload = payload; this.occurredAt = occurredAt;
        this.recebidoEm = recebidoEm; this.particao = particao; this.offsetKafka = offsetKafka;
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
    public long getOffsetKafka() { return offsetKafka; }
}

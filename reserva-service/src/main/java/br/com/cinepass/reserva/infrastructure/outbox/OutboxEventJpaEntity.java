package br.com.cinepass.reserva.infrastructure.outbox;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Linha da tabela de Outbox. É gravada na MESMA transação que persiste o
 * Aggregate Reserva: ou o estado da reserva e o evento são gravados juntos,
 * ou nenhum dos dois (padrão Transactional Outbox).
 */
@Entity
@Table(name = "outbox_events", indexes = @Index(name = "idx_outbox_status_criado", columnList = "status, created_at"))
public class OutboxEventJpaEntity {

    public enum Status { PENDENTE, PUBLICADO }

    @Id private UUID id;
    @Column(name = "aggregate_id", nullable = false) private UUID aggregateId;
    @Column(name = "event_type", nullable = false, length = 60) private String eventType;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Column(name = "correlation_id", length = 80) private String correlationId;
    /** Headers de propagação de trace (JSON) da operação que gerou o evento. */
    @Column(name = "trace_context", columnDefinition = "text") private String traceContext;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
    @Column(name = "published_at") private Instant publishedAt;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 20) private Status status;
    @Column(nullable = false) private int tentativas;

    protected OutboxEventJpaEntity() {}

    public OutboxEventJpaEntity(UUID id, UUID aggregateId, String eventType, String payload, String correlationId, String traceContext) {
        this.id = id; this.aggregateId = aggregateId; this.eventType = eventType; this.payload = payload;
        this.correlationId = correlationId; this.traceContext = traceContext;
        this.createdAt = Instant.now(); this.status = Status.PENDENTE; this.tentativas = 0;
    }

    public void marcarPublicado() { status = Status.PUBLICADO; publishedAt = Instant.now(); }
    public void registrarFalha() { tentativas++; }

    public UUID getId() { return id; }
    public UUID getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public String getCorrelationId() { return correlationId; }
    public String getTraceContext() { return traceContext; }
    public int getTentativas() { return tentativas; }
}

package br.com.cinepass.fidelidade.infrastructure.persistence;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/** Guarda de idempotência: a chave primária é o eventId do evento já processado. */
@Entity
@Table(name = "eventos_processados")
public class EventoProcessadoJpaEntity {
    @Id @Column(name = "event_id") private UUID eventId;
    @Column(name = "event_type", nullable = false, length = 60) private String eventType;
    @Column(name = "processado_em", nullable = false) private Instant processadoEm;

    protected EventoProcessadoJpaEntity() {}

    public EventoProcessadoJpaEntity(UUID eventId, String eventType) {
        this.eventId = eventId; this.eventType = eventType; this.processadoEm = Instant.now();
    }
}

package br.com.cinepass.notificacao.infrastructure.persistence;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notificacoes", indexes = @Index(name = "idx_notificacao_reserva", columnList = "reserva_id"))
public class NotificacaoJpaEntity {
    @Id private UUID id;
    @Column(name = "reserva_id", nullable = false) private UUID reservaId;
    @Column(name = "destinatario_id", nullable = false) private UUID destinatarioId;
    @Column(nullable = false, length = 40) private String tipo;
    @Column(nullable = false, length = 500) private String mensagem;
    @Column(name = "event_id", nullable = false) private UUID eventId;
    @Column(name = "criada_em", nullable = false) private Instant criadaEm;

    protected NotificacaoJpaEntity() {}

    public NotificacaoJpaEntity(UUID id, UUID reservaId, UUID destinatarioId, String tipo, String mensagem, UUID eventId, Instant criadaEm) {
        this.id = id; this.reservaId = reservaId; this.destinatarioId = destinatarioId; this.tipo = tipo;
        this.mensagem = mensagem; this.eventId = eventId; this.criadaEm = criadaEm;
    }

    public UUID getId() { return id; }
    public UUID getReservaId() { return reservaId; }
    public UUID getDestinatarioId() { return destinatarioId; }
    public String getTipo() { return tipo; }
    public String getMensagem() { return mensagem; }
    public UUID getEventId() { return eventId; }
    public Instant getCriadaEm() { return criadaEm; }
}

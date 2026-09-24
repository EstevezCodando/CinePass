package br.com.cinepass.notificacao.domain.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Notificação enviada ao cliente sobre uma mudança no ciclo de vida da sua reserva. */
public class Notificacao {
    private final UUID id;
    private final UUID reservaId;
    private final UUID destinatarioId;
    private final TipoNotificacao tipo;
    private final String mensagem;
    private final UUID eventId;
    private final Instant criadaEm;

    private Notificacao(UUID id, UUID reservaId, UUID destinatarioId, TipoNotificacao tipo, String mensagem,
                        UUID eventId, Instant criadaEm) {
        this.id = Objects.requireNonNull(id);
        this.reservaId = Objects.requireNonNull(reservaId);
        this.destinatarioId = Objects.requireNonNull(destinatarioId, "O destinatário é obrigatório.");
        this.tipo = Objects.requireNonNull(tipo);
        this.mensagem = Objects.requireNonNull(mensagem);
        this.eventId = Objects.requireNonNull(eventId);
        this.criadaEm = Objects.requireNonNull(criadaEm);
    }

    public static Notificacao criar(UUID reservaId, UUID destinatarioId, TipoNotificacao tipo, String mensagem, UUID eventId) {
        return new Notificacao(UUID.randomUUID(), reservaId, destinatarioId, tipo, mensagem, eventId, Instant.now());
    }

    public static Notificacao restaurar(UUID id, UUID reservaId, UUID destinatarioId, TipoNotificacao tipo, String mensagem,
                                        UUID eventId, Instant criadaEm) {
        return new Notificacao(id, reservaId, destinatarioId, tipo, mensagem, eventId, criadaEm);
    }

    public UUID getId() { return id; }
    public UUID getReservaId() { return reservaId; }
    public UUID getDestinatarioId() { return destinatarioId; }
    public TipoNotificacao getTipo() { return tipo; }
    public String getMensagem() { return mensagem; }
    public UUID getEventId() { return eventId; }
    public Instant getCriadaEm() { return criadaEm; }
}

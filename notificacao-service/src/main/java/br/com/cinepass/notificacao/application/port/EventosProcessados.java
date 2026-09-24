package br.com.cinepass.notificacao.application.port;

import java.util.UUID;

/** Registro dos eventos já aplicados, usado para garantir idempotência no consumo. */
public interface EventosProcessados {
    boolean jaProcessado(UUID eventId);
    void registrar(UUID eventId, String eventType);
}

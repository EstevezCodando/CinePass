package br.com.cinepass.fidelidade.application.port;

import java.util.UUID;

/** Registro dos eventos já aplicados, usado para garantir idempotência no consumo. */
public interface EventosProcessados {
    boolean jaProcessado(UUID eventId);
    void registrar(UUID eventId, String eventType);
}

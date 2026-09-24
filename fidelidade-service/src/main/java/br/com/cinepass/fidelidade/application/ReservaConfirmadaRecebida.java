package br.com.cinepass.fidelidade.application;

import java.math.BigDecimal;
import java.util.UUID;

public record ReservaConfirmadaRecebida(UUID eventId, String eventType, UUID reservaId, UUID clienteId,
                                        int ingressos, BigDecimal valorTotal) {
}

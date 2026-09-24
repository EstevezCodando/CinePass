package br.com.cinepass.notificacao.application;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Dados de um evento da reserva já traduzidos para a camada de aplicação. */
public record EventoReservaRecebido(UUID eventId, String eventType, UUID reservaId, UUID clienteId,
                                    List<String> assentos, BigDecimal valorTotal, String correlationId) {
}

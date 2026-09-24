package br.com.cinepass.notificacao.infrastructure.messaging;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Campos do "data" do envelope. Os que não existem em um tipo de evento ficam nulos. */
public record DadosReserva(UUID reservaId, UUID clienteId, UUID sessaoId, List<String> assentos, BigDecimal valorTotal,
                           UUID pagamentoId, UUID ingressoId, String status, String statusAnterior) {
}

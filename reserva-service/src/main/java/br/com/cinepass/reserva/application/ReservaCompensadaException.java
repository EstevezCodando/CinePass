package br.com.cinepass.reserva.application;

import java.util.UUID;

/**
 * A Saga orquestrada pelo Temporal não conseguiu concluir a reserva e executou
 * a compensação: o pagamento foi estornado e a reserva cancelada.
 */
public class ReservaCompensadaException extends RuntimeException {
    private final UUID reservaId;
    private final UUID pagamentoId;

    public ReservaCompensadaException(String mensagem, UUID reservaId, UUID pagamentoId) {
        super(mensagem);
        this.reservaId = reservaId;
        this.pagamentoId = pagamentoId;
    }

    public UUID getReservaId() { return reservaId; }
    public UUID getPagamentoId() { return pagamentoId; }
}

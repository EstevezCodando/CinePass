package br.com.cinepass.reserva.application;

import java.util.UUID;

public class FalhaProcessamentoReservaException extends RuntimeException {
    private final UUID reservaId;
    private final UUID pagamentoId;
    public FalhaProcessamentoReservaException(UUID reservaId, UUID pagamentoId, Throwable cause) {
        super("O pagamento foi aprovado, mas a emissão do ingresso falhou. A transação local será revertida, porém o pagamento remoto continuará confirmado.", cause);
        this.reservaId=reservaId; this.pagamentoId=pagamentoId;
    }
    public UUID getReservaId(){return reservaId;} public UUID getPagamentoId(){return pagamentoId;}
}

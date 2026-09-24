package br.com.cinepass.reserva.application.exception;

public class PagamentoRecusadoException extends RuntimeException {
    public PagamentoRecusadoException() {
        super("Pagamento Recusado!");
    }
}

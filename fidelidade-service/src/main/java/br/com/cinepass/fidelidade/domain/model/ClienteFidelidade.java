package br.com.cinepass.fidelidade.domain.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.UUID;

/**
 * Histórico de consumo do cliente no programa de fidelidade. Cada reserva
 * confirmada soma ingressos, valor gasto e pontos (1 ponto por real inteiro).
 */
public class ClienteFidelidade {
    private final UUID clienteId;
    private int reservasConfirmadas;
    private int ingressosComprados;
    private BigDecimal valorTotalGasto;
    private long pontos;

    private ClienteFidelidade(UUID clienteId, int reservasConfirmadas, int ingressosComprados, BigDecimal valorTotalGasto, long pontos) {
        this.clienteId = Objects.requireNonNull(clienteId, "O id do cliente é obrigatório.");
        this.reservasConfirmadas = reservasConfirmadas;
        this.ingressosComprados = ingressosComprados;
        this.valorTotalGasto = Objects.requireNonNull(valorTotalGasto);
        this.pontos = pontos;
    }

    public static ClienteFidelidade novo(UUID clienteId) {
        return new ClienteFidelidade(clienteId, 0, 0, BigDecimal.ZERO.setScale(2), 0);
    }

    public static ClienteFidelidade restaurar(UUID clienteId, int reservasConfirmadas, int ingressosComprados,
                                              BigDecimal valorTotalGasto, long pontos) {
        return new ClienteFidelidade(clienteId, reservasConfirmadas, ingressosComprados, valorTotalGasto, pontos);
    }

    public void registrarReservaConfirmada(int ingressos, BigDecimal valor) {
        if (ingressos <= 0) throw new IllegalArgumentException("A reserva confirmada precisa ter ingressos.");
        Objects.requireNonNull(valor, "O valor é obrigatório.");
        reservasConfirmadas++;
        ingressosComprados += ingressos;
        valorTotalGasto = valorTotalGasto.add(valor).setScale(2, RoundingMode.HALF_UP);
        pontos += valor.setScale(0, RoundingMode.DOWN).longValue();
    }

    public UUID getClienteId() { return clienteId; }
    public int getReservasConfirmadas() { return reservasConfirmadas; }
    public int getIngressosComprados() { return ingressosComprados; }
    public BigDecimal getValorTotalGasto() { return valorTotalGasto; }
    public long getPontos() { return pontos; }
}

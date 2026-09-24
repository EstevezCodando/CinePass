package br.com.cinepass.fidelidade.infrastructure.persistence;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "clientes_fidelidade")
public class ClienteFidelidadeJpaEntity {
    @Id @Column(name = "cliente_id") private UUID clienteId;
    @Column(name = "reservas_confirmadas", nullable = false) private int reservasConfirmadas;
    @Column(name = "ingressos_comprados", nullable = false) private int ingressosComprados;
    @Column(name = "valor_total_gasto", nullable = false, precision = 14, scale = 2) private BigDecimal valorTotalGasto;
    @Column(nullable = false) private long pontos;
    @Version private long versao;

    protected ClienteFidelidadeJpaEntity() {}

    public ClienteFidelidadeJpaEntity(UUID clienteId) { this.clienteId = clienteId; }

    public void atualizar(int reservasConfirmadas, int ingressosComprados, BigDecimal valorTotalGasto, long pontos) {
        this.reservasConfirmadas = reservasConfirmadas; this.ingressosComprados = ingressosComprados;
        this.valorTotalGasto = valorTotalGasto; this.pontos = pontos;
    }

    public UUID getClienteId() { return clienteId; }
    public int getReservasConfirmadas() { return reservasConfirmadas; }
    public int getIngressosComprados() { return ingressosComprados; }
    public BigDecimal getValorTotalGasto() { return valorTotalGasto; }
    public long getPontos() { return pontos; }
}

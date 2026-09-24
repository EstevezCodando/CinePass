package br.com.cinepass.fidelidade.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ClienteFidelidadeTest {
    @Test
    void acumulaReservasIngressosValorEPontos() {
        var cliente = ClienteFidelidade.novo(UUID.randomUUID());
        cliente.registrarReservaConfirmada(2, new BigDecimal("79.80"));
        cliente.registrarReservaConfirmada(1, new BigDecimal("39.90"));
        assertThat(cliente.getReservasConfirmadas()).isEqualTo(2);
        assertThat(cliente.getIngressosComprados()).isEqualTo(3);
        assertThat(cliente.getValorTotalGasto()).isEqualByComparingTo("119.70");
        assertThat(cliente.getPontos()).isEqualTo(79 + 39);
    }
}

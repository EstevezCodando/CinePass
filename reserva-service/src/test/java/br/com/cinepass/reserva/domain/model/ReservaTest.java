package br.com.cinepass.reserva.domain.model;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReservaTest {
    @Test
    void devePercorrerFluxoFeliz() {
        var reserva=Reserva.criar(new ClienteId(UUID.randomUUID()), new SessaoId(UUID.randomUUID()), List.of(new AssentoId("A1")), new Dinheiro(new BigDecimal("39.90")));
        reserva.aguardarPagamento();
        reserva.confirmarPagamento(UUID.randomUUID());
        reserva.iniciarEmissaoIngresso();
        reserva.confirmar(UUID.randomUUID());
        assertEquals(StatusReserva.CONFIRMADA, reserva.getStatus());
    }
}

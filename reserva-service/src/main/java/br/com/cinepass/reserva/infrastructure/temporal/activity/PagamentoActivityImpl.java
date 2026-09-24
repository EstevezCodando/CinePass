package br.com.cinepass.reserva.infrastructure.temporal.activity;

import br.com.cinepass.reserva.application.port.PagamentoGateway;
import io.temporal.spring.boot.ActivityImpl;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
@ActivityImpl(taskQueues = "cinepass-reserva")
public class PagamentoActivityImpl implements PagamentoActivity {
    private final PagamentoGateway pagamentoGateway;

    public PagamentoActivityImpl(PagamentoGateway pagamentoGateway) {
        this.pagamentoGateway = pagamentoGateway;
    }

    @Override
    public Resultado cobrar(UUID reservaId, UUID clienteId, BigDecimal valor, boolean simularRecusa) {
        var pagamento = pagamentoGateway.cobrar(reservaId, valor, simularRecusa);
        return new Resultado(pagamento.pagamentoId(),pagamento.status());
    }

    @Override
    public Resultado estornar(UUID pagamentoId) {
        var pagamento = pagamentoGateway.estornar(pagamentoId);
        return new Resultado(pagamento.pagamentoId(),pagamento.status());
    }
}

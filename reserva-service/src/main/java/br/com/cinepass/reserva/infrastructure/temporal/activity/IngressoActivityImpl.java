package br.com.cinepass.reserva.infrastructure.temporal.activity;

import br.com.cinepass.reserva.application.port.IngressoGateway;
import io.temporal.spring.boot.ActivityImpl;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@ActivityImpl(taskQueues = "cinepass-reserva")
public class IngressoActivityImpl implements IngressoActivity{
    private final IngressoGateway ingressoGateway;

    public IngressoActivityImpl(IngressoGateway ingressoGateway) {
        this.ingressoGateway = ingressoGateway;
    }

    @Override
    public Resultado emitir(UUID reservaId, UUID sessaoId, List<String> assentos, boolean simularFalha) {

        var ingresso = ingressoGateway.emitir(
                reservaId,
                sessaoId,
                assentos,
                simularFalha
        );
        return new Resultado(ingresso.ingressoId(),ingresso.codigo(),ingresso.status());
    }
}

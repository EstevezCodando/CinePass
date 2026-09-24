package br.com.cinepass.reserva.infrastructure.http;

import br.com.cinepass.reserva.application.FalhaIntegracaoException;
import br.com.cinepass.reserva.application.port.IngressoGateway;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.UUID;

@Component
public class IngressoHttpGateway implements IngressoGateway {
    private final RestClient restClient;

    public IngressoHttpGateway(@LoadBalanced RestClient.Builder builder) {
        this.restClient = builder.baseUrl("http://INGRESSO-SERVICE").build();
    }

    @Override
    public IngressoResultado emitir(UUID reservaId, UUID sessaoId, List<String> assentos, boolean simularFalha) {
        try {
            IngressoResponse response = restClient.post()
                    .uri("/api/ingressos")
                    .body(new IngressoRequest(reservaId, sessaoId, assentos, simularFalha))
                    .retrieve()
                    .body(IngressoResponse.class);
            if (response == null) throw new FalhaIntegracaoException("Ingresso respondeu sem corpo.", null);
            return new IngressoResultado(response.ingressoId(), response.codigo(), response.status());
        } catch (RestClientException ex) {
            throw new FalhaIntegracaoException("Falha ao chamar ingresso-service.", ex);
        }
    }

    private record IngressoRequest(UUID reservaId, UUID sessaoId, List<String> assentos, boolean simularFalha) {}
    private record IngressoResponse(UUID ingressoId, UUID reservaId, UUID sessaoId, List<String> assentos, String codigo, String status) {}
}

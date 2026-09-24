package br.com.cinepass.reserva.infrastructure.http;

import br.com.cinepass.reserva.application.FalhaIntegracaoException;
import br.com.cinepass.reserva.application.port.PagamentoGateway;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class PagamentoHttpGateway implements PagamentoGateway {
    private final RestClient restClient;

    public PagamentoHttpGateway(@LoadBalanced RestClient.Builder builder) {
        this.restClient = builder.baseUrl("http://PAGAMENTO-SERVICE").build();
    }

    @Override
    public PagamentoResultado cobrar(UUID reservaId, BigDecimal valor, boolean simularRecusa) {
        try {
            PagamentoResponse response = restClient.post()
                    .uri("/api/pagamentos")
                    .body(new PagamentoRequest(reservaId, valor, simularRecusa))
                    .retrieve()
                    .body(PagamentoResponse.class);
            if (response == null) throw new FalhaIntegracaoException("Pagamento respondeu sem corpo.", null);
            return new PagamentoResultado(response.pagamentoId(), response.status());
        } catch (RestClientException ex) {
            throw new FalhaIntegracaoException("Falha ao chamar pagamento-service.", ex);
        }
    }

    @Override
    public PagamentoResultado estornar(
            UUID pagamentoId
    ) {

        try {

            PagamentoResponse response =
                    restClient.post()
                            .uri(
                                    "/api/pagamentos/{id}/estorno",
                                    pagamentoId
                            )
                            .retrieve()
                            .body(PagamentoResponse.class);

            if (response == null) {
                throw new FalhaIntegracaoException(
                        "Pagamento respondeu sem corpo no estorno.",
                        null
                );
            }

            return new PagamentoResultado(
                    response.pagamentoId(),
                    response.status()
            );

        } catch (RestClientException ex) {

            throw new FalhaIntegracaoException(
                    "Falha ao estornar pagamento.",
                    ex
            );
        }
    }
    private record PagamentoRequest(UUID reservaId, BigDecimal valor, boolean simularRecusa) {}
    private record PagamentoResponse(UUID pagamentoId, UUID reservaId, BigDecimal valor, String status) {}
}

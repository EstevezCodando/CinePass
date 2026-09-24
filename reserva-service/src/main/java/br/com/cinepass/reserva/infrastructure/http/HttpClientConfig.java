package br.com.cinepass.reserva.infrastructure.http;

import io.micrometer.observation.ObservationRegistry;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestClient;

@Configuration
public class HttpClientConfig {

    @Bean
    @Primary
    RestClient.Builder restClientBuilder(ObjectProvider<ObservationRegistry> observationRegistry) {
        return instrumentado(observationRegistry);
    }

    @Bean
    @LoadBalanced
    RestClient.Builder loadBalancedRestClientBuilder(ObjectProvider<ObservationRegistry> observationRegistry) {
        return instrumentado(observationRegistry);
    }

    /**
     * Builders criados à mão não recebem o ObservationRegistry do Spring Boot:
     * sem ele, o contexto de trace não seguiria para pagamento-service e
     * ingresso-service. O interceptor repassa também o X-Correlation-Id.
     */
    private RestClient.Builder instrumentado(ObjectProvider<ObservationRegistry> observationRegistry) {
        return RestClient.builder()
                .observationRegistry(observationRegistry.getIfAvailable(() -> ObservationRegistry.NOOP))
                .requestInterceptor(propagarCorrelationId());
    }

    private ClientHttpRequestInterceptor propagarCorrelationId() {
        return (request, body, execution) -> {
            String correlationId = MDC.get("correlationId");
            if (correlationId != null) {
                request.getHeaders().set("X-Correlation-Id", correlationId);
            }
            return execution.execute(request, body);
        };
    }
}

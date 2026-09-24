package br.com.cinepass.gateway;

import net.logstash.logback.argument.StructuredArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Primeiro elo da correlação: usa o X-Correlation-Id enviado pelo cliente ou
 * gera um novo, repassa ao serviço de destino e devolve na resposta. Como o
 * Gateway é reativo, o correlationId vai no log como campo estruturado (e não
 * pelo MDC), para aparecer como campo pesquisável no Kibana.
 */
@Component
public class CorrelationIdGatewayFilter implements GlobalFilter, Ordered {
    private static final Logger log = LoggerFactory.getLogger(CorrelationIdGatewayFilter.class);
    public static final String HEADER = "X-Correlation-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String recebido = exchange.getRequest().getHeaders().getFirst(HEADER);
        String correlationId = (recebido == null || recebido.isBlank()) ? UUID.randomUUID().toString() : recebido;
        long inicio = System.currentTimeMillis();

        var request = exchange.getRequest().mutate().header(HEADER, correlationId).build();
        exchange.getResponse().beforeCommit(() -> {
            exchange.getResponse().getHeaders().set(HEADER, correlationId);
            return Mono.empty();
        });
        log.info("gateway.request.inicio method={} path={} {}", request.getMethod(), request.getURI().getPath(),
                StructuredArguments.keyValue("correlationId", correlationId));

        return chain.filter(exchange.mutate().request(request).build())
                .doFinally(signal -> log.info("gateway.request.fim method={} path={} status={} durationMs={} {}",
                        request.getMethod(), request.getURI().getPath(), exchange.getResponse().getStatusCode(),
                        System.currentTimeMillis() - inicio, StructuredArguments.keyValue("correlationId", correlationId)));
    }

    @Override
    public int getOrder() {
        return -100;
    }
}

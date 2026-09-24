package br.com.cinepass.reserva.infrastructure.temporal;

import io.temporal.client.WorkflowClientOptions;
import io.temporal.spring.boot.TemporalOptionsCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class TemporalConfig {

    /** Registra o propagador do correlationId no cliente (e nos workers criados a partir dele). */
    @Bean
    TemporalOptionsCustomizer<WorkflowClientOptions.Builder> correlationIdNoTemporal() {
        return builder -> builder.setContextPropagators(List.of(new CorrelationIdContextPropagator()));
    }
}

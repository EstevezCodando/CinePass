package br.com.cinepass.reserva.infrastructure.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Leva o contexto de tracing da requisição HTTP até a publicação no Kafka.
 * A outbox é publicada depois, em outra thread (@Scheduled); sem isso o trace
 * do Zipkin "quebraria" entre a requisição e o consumo do evento.
 */
@Component
public class TraceContextOutbox {

    private final ObjectProvider<Tracer> tracer;
    private final ObjectProvider<Propagator> propagator;
    private final JsonMapper jsonMapper;

    public TraceContextOutbox(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator, JsonMapper jsonMapper) {
        this.tracer = tracer;
        this.propagator = propagator;
        this.jsonMapper = jsonMapper;
    }

    public String capturar() {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        if (t == null || p == null || t.currentSpan() == null) {
            return null;
        }
        Map<String, String> headers = new LinkedHashMap<>();
        p.inject(t.currentSpan().context(), headers, Map::put);
        return jsonMapper.writeValueAsString(headers);
    }

    /** Abre um span de publicação filho do trace original; o chamador deve encerrá-lo. */
    public Span iniciarSpanDePublicacao(String traceContext, String nome) {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        if (t == null || p == null) {
            return null;
        }
        Map<String, String> headers = Map.of();
        if (traceContext != null) {
            headers = jsonMapper.readValue(traceContext, new TypeReference<Map<String, String>>() { });
        }
        Map<String, String> carrier = headers;
        return p.extract(carrier, Map::get).name(nome).kind(Span.Kind.PRODUCER).start();
    }

    public Tracer.SpanInScope emEscopo(Span span) {
        Tracer t = tracer.getIfAvailable();
        return (t == null || span == null) ? null : t.withSpan(span);
    }
}

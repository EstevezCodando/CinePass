package br.com.cinepass.reserva.infrastructure.temporal;

import io.temporal.api.common.v1.Payload;
import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DefaultDataConverter;
import org.slf4j.MDC;

import java.util.Map;

/**
 * Leva o correlationId da requisição HTTP (MDC) para o workflow e para as
 * activities do Temporal, que rodam em outras threads.
 *
 * Na chamada ao workflow o valor é lido do MDC e vai no header do Temporal; em
 * cada activity ele volta para o MDC. Assim os logs das activities, as chamadas
 * HTTP ao pagamento/ingresso (interceptor do RestClient) e os eventos gravados
 * na outbox recebem o mesmo correlationId do fluxo síncrono.
 */
public class CorrelationIdContextPropagator implements ContextPropagator {

    static final String CHAVE = "correlationId";

    @Override
    public String getName() {
        return "cinepass-correlation-id";
    }

    @Override
    public Map<String, Payload> serializeContext(Object contexto) {
        // sempre serializa (vazio quando não há valor), para que a activity
        // seguinte limpe um correlationId antigo da thread reaproveitada
        String valor = contexto instanceof String texto ? texto : "";
        return Map.of(CHAVE, DefaultDataConverter.STANDARD_INSTANCE.toPayload(valor).orElseThrow());
    }

    @Override
    public Object deserializeContext(Map<String, Payload> header) {
        Payload payload = header.get(CHAVE);
        return payload == null ? "" : DefaultDataConverter.STANDARD_INSTANCE.fromPayload(payload, String.class, String.class);
    }

    @Override
    public Object getCurrentContext() {
        return MDC.get(CHAVE);
    }

    @Override
    public void setCurrentContext(Object contexto) {
        // as threads de activity são reaproveitadas: o reservaId de uma execução
        // anterior (nem o eventId/eventType) não pode aparecer nos logs da próxima
        MDC.remove("reservaId");
        MDC.remove("eventId");
        MDC.remove("eventType");
        if (contexto instanceof String valor && !valor.isBlank()) {
            MDC.put(CHAVE, valor);
        } else {
            MDC.remove(CHAVE);
        }
    }
}

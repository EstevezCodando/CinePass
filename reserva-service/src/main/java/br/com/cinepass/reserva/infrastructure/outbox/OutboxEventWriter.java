package br.com.cinepass.reserva.infrastructure.outbox;

import br.com.cinepass.reserva.application.port.EventosDeDominioPublisher;
import br.com.cinepass.reserva.domain.shared.DomainEvent;
import br.com.cinepass.reserva.infrastructure.messaging.DomainEventMapper;
import br.com.cinepass.reserva.infrastructure.messaging.ReservaEventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.UUID;

/**
 * Grava os eventos de domínio na tabela de outbox. Exige uma transação já
 * aberta (MANDATORY): só faz sentido junto com a transação que persiste o
 * Aggregate, para que estado e evento sejam gravados atomicamente.
 */
@Component
public class OutboxEventWriter implements EventosDeDominioPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventWriter.class);

    private final SpringDataOutboxRepository repository;
    private final DomainEventMapper mapper;
    private final JsonMapper jsonMapper;
    private final TraceContextOutbox traceContext;

    public OutboxEventWriter(SpringDataOutboxRepository repository, DomainEventMapper mapper, JsonMapper jsonMapper,
                             TraceContextOutbox traceContext) {
        this.repository = repository;
        this.mapper = mapper;
        this.jsonMapper = jsonMapper;
        this.traceContext = traceContext;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(List<DomainEvent> eventos, UUID reservaId) {
        String correlationId = MDC.get("correlationId");
        String contextoDeTrace = traceContext.capturar();
        for (DomainEvent evento : eventos) {
            ReservaEventEnvelope envelope = mapper.paraEnvelope(evento, reservaId, correlationId);
            String payload = jsonMapper.writeValueAsString(envelope);
            repository.save(new OutboxEventJpaEntity(evento.eventId(), reservaId, evento.eventType(), payload, correlationId, contextoDeTrace));
            MDC.put("eventId", evento.eventId().toString());
            MDC.put("eventType", evento.eventType());
            log.info("reserva.outbox.registrado reservaId={} eventId={} eventType={} correlationId={}",
                    reservaId, evento.eventId(), evento.eventType(), correlationId);
            MDC.remove("eventId");
            MDC.remove("eventType");
        }
    }
}

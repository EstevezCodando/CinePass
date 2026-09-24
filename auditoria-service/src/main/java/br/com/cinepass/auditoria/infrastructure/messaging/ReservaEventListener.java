package br.com.cinepass.auditoria.infrastructure.messaging;

import br.com.cinepass.auditoria.application.AuditoriaApplicationService;
import br.com.cinepass.auditoria.application.EventoParaAuditoria;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Consome todos os eventos do tópico reserva.eventos e os registra na trilha de auditoria. */
@Component
public class ReservaEventListener {
    private static final Logger log = LoggerFactory.getLogger(ReservaEventListener.class);

    private final AuditoriaApplicationService service;
    private final JsonMapper jsonMapper;

    public ReservaEventListener(AuditoriaApplicationService service, JsonMapper jsonMapper) {
        this.service = service;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "${cinepass.kafka.topics.reserva-eventos}", groupId = "${spring.application.name}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        ReservaEventEnvelope envelope;
        try {
            envelope = jsonMapper.readValue(record.value(), ReservaEventEnvelope.class);
        } catch (RuntimeException ex) {
            log.error("kafka.evento.falha service=auditoria-service etapa=desserializacao partition={} offset={} key={} motivo={}",
                    record.partition(), record.offset(), record.key(), ex.getMessage());
            throw ex;
        }
        try {
            MDC.put("correlationId", envelope.correlationId());
            MDC.put("reservaId", String.valueOf(envelope.reservaId()));
            MDC.put("eventId", String.valueOf(envelope.eventId()));
            MDC.put("eventType", envelope.eventType());
            log.info("kafka.evento.recebido service=auditoria-service partition={} offset={} key={} eventId={} eventType={} reservaId={} correlationId={}",
                    record.partition(), record.offset(), record.key(), envelope.eventId(), envelope.eventType(),
                    envelope.reservaId(), envelope.correlationId());
            service.registrar(new EventoParaAuditoria(envelope.eventId(), envelope.reservaId(), envelope.eventType(),
                    envelope.correlationId(), record.value(), envelope.occurredAt(), record.partition(), record.offset()));
        } catch (RuntimeException ex) {
            log.error("kafka.evento.falha service=auditoria-service etapa=processamento eventId={} eventType={} reservaId={} motivo={}",
                    envelope.eventId(), envelope.eventType(), envelope.reservaId(), ex.getMessage(), ex);
            throw ex;
        } finally {
            MDC.remove("correlationId");
            MDC.remove("reservaId");
            MDC.remove("eventId");
            MDC.remove("eventType");
        }
    }
}

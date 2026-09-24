package br.com.cinepass.fidelidade.infrastructure.messaging;

import br.com.cinepass.fidelidade.application.FidelidadeApplicationService;
import br.com.cinepass.fidelidade.application.ReservaConfirmadaRecebida;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Só ReservaConfirmada altera o histórico do cliente. Os outros tipos já são
 * descartados pelo filtro do container (header eventType); a checagem abaixo
 * cobre mensagens sem esse header, como as republicadas a partir do DLT.
 */
@Component
public class ReservaEventListener {
    private static final Logger log = LoggerFactory.getLogger(ReservaEventListener.class);

    private final FidelidadeApplicationService service;
    private final JsonMapper jsonMapper;

    public ReservaEventListener(FidelidadeApplicationService service, JsonMapper jsonMapper) {
        this.service = service;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "${cinepass.kafka.topics.reserva-eventos}", groupId = "${spring.application.name}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record) {
        ReservaEventEnvelope envelope;
        try {
            envelope = jsonMapper.readerFor(ReservaEventEnvelope.class)
                    .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).readValue(record.value());
        } catch (RuntimeException ex) {
            log.error("kafka.evento.falha service=fidelidade-service etapa=desserializacao partition={} offset={} key={} motivo={}",
                    record.partition(), record.offset(), record.key(), ex.getMessage());
            throw ex;
        }
        try {
            MDC.put("correlationId", envelope.correlationId());
            MDC.put("reservaId", String.valueOf(envelope.reservaId()));
            MDC.put("eventId", String.valueOf(envelope.eventId()));
            MDC.put("eventType", envelope.eventType());
            log.info("kafka.evento.recebido service=fidelidade-service partition={} offset={} key={} eventId={} eventType={} reservaId={} correlationId={}",
                    record.partition(), record.offset(), record.key(), envelope.eventId(), envelope.eventType(),
                    envelope.reservaId(), envelope.correlationId());
            if (!"ReservaConfirmada".equals(envelope.eventType())) {
                log.info("fidelidade.evento.ignorado eventId={} eventType={} motivo=nao_altera_historico", envelope.eventId(), envelope.eventType());
                return;
            }
            DadosReserva dados = jsonMapper.convertValue(envelope.data(), DadosReserva.class);
            service.registrarReservaConfirmada(new ReservaConfirmadaRecebida(envelope.eventId(), envelope.eventType(),
                    envelope.reservaId(), dados.clienteId(), dados.assentos().size(), dados.valorTotal()));
        } catch (RuntimeException ex) {
            log.error("kafka.evento.falha service=fidelidade-service etapa=processamento eventId={} eventType={} reservaId={} motivo={}",
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

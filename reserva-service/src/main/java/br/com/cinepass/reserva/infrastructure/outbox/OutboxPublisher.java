package br.com.cinepass.reserva.infrastructure.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Publicador do Transactional Outbox. A cada ciclo busca os eventos PENDENTE,
 * publica no Kafka na ordem em que foram gravados (esperando a confirmação do
 * broker antes do próximo) e marca como PUBLICADO. Se um envio falhar, o lote
 * para ali: os eventos seguintes, que podem ser da mesma reserva, esperam o
 * próximo ciclo, preservando a ordem. A entrega é at-least-once, por isso os
 * consumidores são idempotentes.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int LOTE = 20;
    private static final long ACK_TIMEOUT_SEGUNDOS = 10;
    private static final long CHAVE_LOCK_PUBLICADOR = 7_314_002L;

    private final SpringDataOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TraceContextOutbox traceContext;
    private final String topic;

    public OutboxPublisher(SpringDataOutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                           TraceContextOutbox traceContext,
                           @Value("${cinepass.kafka.topics.reserva-eventos}") String topic) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.traceContext = traceContext;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${cinepass.outbox.poll-interval-ms:500}")
    @Transactional
    public void publicarPendentes() {
        if (!repository.tentarLockDoPublicador(CHAVE_LOCK_PUBLICADOR)) {
            return;
        }
        List<OutboxEventJpaEntity> lote = repository.buscarLotePendente(LOTE);
        if (lote.isEmpty()) {
            return;
        }
        log.info("reserva.outbox.lote.inicio quantidade={}", lote.size());
        for (OutboxEventJpaEntity evento : lote) {
            if (!publicar(evento)) {
                break;
            }
        }
    }

    private boolean publicar(OutboxEventJpaEntity evento) {
        String key = evento.getAggregateId().toString();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, evento.getPayload());
        record.headers()
                .add(new RecordHeader("eventId", evento.getId().toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventType", evento.getEventType().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("correlationId", String.valueOf(evento.getCorrelationId()).getBytes(StandardCharsets.UTF_8)));

        MDC.put("reservaId", key);
        MDC.put("eventId", evento.getId().toString());
        MDC.put("eventType", evento.getEventType());
        if (evento.getCorrelationId() != null) {
            MDC.put("correlationId", evento.getCorrelationId());
        }
        Span span = traceContext.iniciarSpanDePublicacao(evento.getTraceContext(), "outbox publicar " + evento.getEventType());
        try (Tracer.SpanInScope ignored = traceContext.emEscopo(span)) {
            RecordMetadata meta = kafkaTemplate.send(record).get(ACK_TIMEOUT_SEGUNDOS, TimeUnit.SECONDS).getRecordMetadata();
            evento.marcarPublicado();
            log.info("reserva.outbox.publicado reservaId={} eventId={} eventType={} topic={} partition={} offset={}",
                    key, evento.getId(), evento.getEventType(), topic, meta.partition(), meta.offset());
            return true;
        } catch (Exception ex) {
            evento.registrarFalha();
            if (span != null) {
                span.error(ex);
            }
            log.error("reserva.outbox.falha reservaId={} eventId={} eventType={} tentativas={} motivo={}",
                    key, evento.getId(), evento.getEventType(), evento.getTentativas(), ex.getMessage(), ex);
            return false;
        } finally {
            if (span != null) {
                span.end();
            }
            MDC.remove("reservaId");
            MDC.remove("eventId");
            MDC.remove("eventType");
            MDC.remove("correlationId");
        }
    }
}

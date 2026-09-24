package br.com.cinepass.notificacao.infrastructure.messaging;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;


/**
 * Resiliência do consumidor: até 3 novas tentativas (1 s de intervalo) e, se a
 * mensagem continuar falhando, envio ao dead-letter topic deste serviço, com a
 * mesma chave (reservaId). A concorrência (3 threads) é menor que o número de
 * partições (6): cada thread recebe partições inteiras, então reservas
 * diferentes são processadas em paralelo e a ordem de uma mesma reserva é mantida.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                 @Value("${cinepass.kafka.topics.dlt}") String dltTopic) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            log.error("kafka.evento.dlt.encaminhado topicOrigem={} partition={} offset={} key={} destino={} motivo={}",
                    record.topic(), record.partition(), record.offset(), record.key(), dltTopic, causaRaiz(ex));
            return new TopicPartition(dltTopic, -1);
        });
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
        handler.setRetryListeners((record, ex, tentativa) ->
                log.warn("kafka.evento.retry topic={} key={} tentativa={} motivo={}",
                        record.topic(), record.key(), tentativa, causaRaiz(ex)));
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler kafkaErrorHandler,
            @Value("${cinepass.kafka.listener.concorrencia:3}") int concorrencia) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.setConcurrency(concorrencia);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // Factory própria: a observação (continuação do trace a partir dos headers
        // da mensagem) precisa ser ligada aqui, a propriedade do Boot não se aplica.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    private static String causaRaiz(Exception ex) {
        Throwable causa = NestedExceptionUtils.getMostSpecificCause(ex);
        return causa.getClass().getSimpleName() + ": " + causa.getMessage();
    }
}

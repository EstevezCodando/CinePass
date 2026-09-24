package br.com.cinepass.reserva.infrastructure.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Tópico dos eventos do ciclo de vida da reserva. As partições permitem
 * processar reservas diferentes em paralelo; a chave (reservaId) mantém os
 * eventos de uma mesma reserva sempre na mesma partição, em ordem.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic reservaEventosTopic(@Value("${cinepass.kafka.topics.reserva-eventos}") String nome,
                                        @Value("${cinepass.kafka.topics.reserva-eventos-particoes:6}") int particoes) {
        return TopicBuilder.name(nome).partitions(particoes).replicas(1).build();
    }
}

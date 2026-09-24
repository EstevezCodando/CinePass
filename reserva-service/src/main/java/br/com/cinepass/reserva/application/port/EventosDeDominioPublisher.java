package br.com.cinepass.reserva.application.port;

import br.com.cinepass.reserva.domain.shared.DomainEvent;

import java.util.List;
import java.util.UUID;

/**
 * Porta de saída para os eventos de domínio da Reserva. A implementação
 * (Transactional Outbox) grava os eventos na mesma transação do Aggregate.
 */
public interface EventosDeDominioPublisher {
    void registrar(List<DomainEvent> eventos, UUID reservaId);
}

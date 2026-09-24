package br.com.cinepass.fidelidade.infrastructure.persistence;

import br.com.cinepass.fidelidade.application.port.EventosProcessados;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class EventosProcessadosJpaAdapter implements EventosProcessados {
    private final SpringDataEventoProcessadoRepository repository;

    public EventosProcessadosJpaAdapter(SpringDataEventoProcessadoRepository repository) { this.repository = repository; }

    @Override public boolean jaProcessado(UUID eventId) { return repository.existsById(eventId); }
    @Override public void registrar(UUID eventId, String eventType) { repository.save(new EventoProcessadoJpaEntity(eventId, eventType)); }
}

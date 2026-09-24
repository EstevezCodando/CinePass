package br.com.cinepass.auditoria.infrastructure.persistence;

import br.com.cinepass.auditoria.domain.model.RegistroAuditoria;
import br.com.cinepass.auditoria.domain.repository.RegistroAuditoriaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class RegistroAuditoriaRepositoryJpaAdapter implements RegistroAuditoriaRepository {
    private final SpringDataRegistroAuditoriaRepository repository;

    public RegistroAuditoriaRepositoryJpaAdapter(SpringDataRegistroAuditoriaRepository repository) { this.repository = repository; }

    @Override
    public RegistroAuditoria salvar(RegistroAuditoria r) {
        return paraDominio(repository.save(new RegistroAuditoriaJpaEntity(r.getId(), r.getEventId(), r.getReservaId(),
                r.getEventType(), r.getCorrelationId(), r.getPayload(), r.getOccurredAt(), r.getRecebidoEm(), r.getParticao(), r.getOffset())));
    }

    @Override public boolean existePorEventId(UUID eventId) { return repository.existsByEventId(eventId); }

    @Override public List<RegistroAuditoria> listar() { return repository.findAll().stream().map(this::paraDominio).toList(); }

    @Override
    public List<RegistroAuditoria> listarPorReserva(UUID reservaId) {
        return repository.findByReservaIdOrderByRecebidoEmAsc(reservaId).stream().map(this::paraDominio).toList();
    }

    private RegistroAuditoria paraDominio(RegistroAuditoriaJpaEntity e) {
        return RegistroAuditoria.restaurar(e.getId(), e.getEventId(), e.getReservaId(), e.getEventType(), e.getCorrelationId(),
                e.getPayload(), e.getOccurredAt(), e.getRecebidoEm(), e.getParticao(), e.getOffsetKafka());
    }
}

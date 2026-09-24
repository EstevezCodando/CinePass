package br.com.cinepass.notificacao.infrastructure.persistence;

import br.com.cinepass.notificacao.domain.model.Notificacao;
import br.com.cinepass.notificacao.domain.model.TipoNotificacao;
import br.com.cinepass.notificacao.domain.repository.NotificacaoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class NotificacaoRepositoryJpaAdapter implements NotificacaoRepository {
    private final SpringDataNotificacaoRepository repository;

    public NotificacaoRepositoryJpaAdapter(SpringDataNotificacaoRepository repository) { this.repository = repository; }

    @Override
    public Notificacao salvar(Notificacao n) {
        return paraDominio(repository.save(new NotificacaoJpaEntity(n.getId(), n.getReservaId(), n.getDestinatarioId(),
                n.getTipo().name(), n.getMensagem(), n.getEventId(), n.getCriadaEm())));
    }

    @Override public List<Notificacao> listar() { return repository.findAll().stream().map(this::paraDominio).toList(); }

    @Override
    public List<Notificacao> listarPorReserva(UUID reservaId) {
        return repository.findByReservaIdOrderByCriadaEmAsc(reservaId).stream().map(this::paraDominio).toList();
    }

    private Notificacao paraDominio(NotificacaoJpaEntity e) {
        return Notificacao.restaurar(e.getId(), e.getReservaId(), e.getDestinatarioId(), TipoNotificacao.valueOf(e.getTipo()),
                e.getMensagem(), e.getEventId(), e.getCriadaEm());
    }
}

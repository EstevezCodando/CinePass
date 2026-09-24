package br.com.cinepass.notificacao.domain.repository;

import br.com.cinepass.notificacao.domain.model.Notificacao;

import java.util.List;
import java.util.UUID;

public interface NotificacaoRepository {
    Notificacao salvar(Notificacao notificacao);
    List<Notificacao> listar();
    List<Notificacao> listarPorReserva(UUID reservaId);
}

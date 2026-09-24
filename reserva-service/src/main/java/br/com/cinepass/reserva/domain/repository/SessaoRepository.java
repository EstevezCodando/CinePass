package br.com.cinepass.reserva.domain.repository;

import br.com.cinepass.reserva.domain.model.Sessao;
import br.com.cinepass.reserva.domain.model.SessaoId;
import java.util.List;
import java.util.Optional;

public interface SessaoRepository {
    Optional<Sessao> buscarPorId(SessaoId id);
    Sessao salvar(Sessao sessao);
    List<Sessao> listar();
}

package br.com.cinepass.fidelidade.domain.repository;

import br.com.cinepass.fidelidade.domain.model.ClienteFidelidade;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ClienteFidelidadeRepository {
    ClienteFidelidade salvar(ClienteFidelidade cliente);
    Optional<ClienteFidelidade> buscarPorId(UUID clienteId);
    List<ClienteFidelidade> listar();
}

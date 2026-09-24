package br.com.cinepass.fidelidade.infrastructure.persistence;

import br.com.cinepass.fidelidade.domain.model.ClienteFidelidade;
import br.com.cinepass.fidelidade.domain.repository.ClienteFidelidadeRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ClienteFidelidadeRepositoryJpaAdapter implements ClienteFidelidadeRepository {
    private final SpringDataClienteFidelidadeRepository repository;

    public ClienteFidelidadeRepositoryJpaAdapter(SpringDataClienteFidelidadeRepository repository) { this.repository = repository; }

    @Override
    public ClienteFidelidade salvar(ClienteFidelidade c) {
        ClienteFidelidadeJpaEntity entity = repository.findById(c.getClienteId())
                .orElseGet(() -> new ClienteFidelidadeJpaEntity(c.getClienteId()));
        entity.atualizar(c.getReservasConfirmadas(), c.getIngressosComprados(), c.getValorTotalGasto(), c.getPontos());
        return paraDominio(repository.save(entity));
    }

    @Override public Optional<ClienteFidelidade> buscarPorId(UUID clienteId) { return repository.findById(clienteId).map(this::paraDominio); }

    @Override public List<ClienteFidelidade> listar() { return repository.findAll().stream().map(this::paraDominio).toList(); }

    private ClienteFidelidade paraDominio(ClienteFidelidadeJpaEntity e) {
        return ClienteFidelidade.restaurar(e.getClienteId(), e.getReservasConfirmadas(), e.getIngressosComprados(),
                e.getValorTotalGasto(), e.getPontos());
    }
}

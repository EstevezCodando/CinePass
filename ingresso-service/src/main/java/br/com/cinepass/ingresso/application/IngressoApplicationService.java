package br.com.cinepass.ingresso.application;

import br.com.cinepass.ingresso.domain.model.*;
import br.com.cinepass.ingresso.domain.repository.IngressoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class IngressoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(IngressoApplicationService.class);
    private final IngressoRepository repository;

    public IngressoApplicationService(IngressoRepository repository) { this.repository = repository; }

    @Transactional
    public Ingresso emitir(EmitirIngressoCommand command) {
        log.info("ingresso.emissao.inicio reservaId={} assentos={}", command.reservaId(), command.assentos());
        if (command.simularFalha()) {
            log.error("ingresso.emissao.falha reservaId={} motivo=falha_simulada", command.reservaId());
            throw new EmissaoIngressoException("Falha proposital na emissão do ingresso para demonstrar transação distribuída.");
        }
        Ingresso ingresso = repository.salvar(Ingresso.emitir(new ReservaId(command.reservaId()), new SessaoId(command.sessaoId()), command.assentos()));
        log.info("ingresso.emissao.fim reservaId={} ingressoId={} codigo={}", command.reservaId(), ingresso.getId().valor(), ingresso.getCodigo().valor());
        return ingresso;
    }

    @Transactional
    public Ingresso cancelar(UUID ingressoId) {
        Ingresso ingresso = repository.buscarPorId(new IngressoId(ingressoId))
                .orElseThrow(() -> new IngressoNaoEncontradoException(ingressoId));
        ingresso.cancelar();
        log.info("ingresso.cancelamento ingressoId={} reservaId={}", ingressoId, ingresso.getReservaId().valor());
        return repository.salvar(ingresso);
    }

    @Transactional(readOnly = true)
    public Ingresso buscarPorReserva(UUID reservaId) {
        return repository.buscarPorReserva(new ReservaId(reservaId))
                .orElseThrow(() -> new IngressoNaoEncontradoException(reservaId));
    }
}

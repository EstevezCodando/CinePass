package br.com.cinepass.reserva.application;

import br.com.cinepass.reserva.application.port.EventosDeDominioPublisher;
import br.com.cinepass.reserva.application.port.IngressoGateway;
import br.com.cinepass.reserva.application.port.PagamentoGateway;
import br.com.cinepass.reserva.domain.model.*;
import br.com.cinepass.reserva.domain.repository.ReservaRepository;
import br.com.cinepass.reserva.domain.repository.SessaoRepository;
import br.com.cinepass.reserva.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class ReservaApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ReservaApplicationService.class);
    private final ReservaRepository reservaRepository;
    private final SessaoRepository sessaoRepository;
    private final PagamentoGateway pagamentoGateway;
    private final IngressoGateway ingressoGateway;
    private final EventosDeDominioPublisher eventos;

    public ReservaApplicationService(ReservaRepository reservaRepository, SessaoRepository sessaoRepository,
                                     PagamentoGateway pagamentoGateway, IngressoGateway ingressoGateway,
                                     EventosDeDominioPublisher eventos) {
        this.reservaRepository=reservaRepository; this.sessaoRepository=sessaoRepository;
        this.pagamentoGateway=pagamentoGateway; this.ingressoGateway=ingressoGateway;
        this.eventos=eventos;
    }

    /**
     * Esta transação é propositalmente longa para a aula. Ela protege apenas o banco do reserva-service.
     * Os commits realizados em pagamento-service e ingresso-service não participam desta transação local.
     */
    @Transactional
    //ACID
    public ReservaDetalhe realizar(RealizarReservaCommand command) {
        log.info("reserva.realizacao.inicio clienteId={} sessaoId={} assentos={}", command.clienteId(), command.sessaoId(), command.assentos());
        Sessao sessao = sessaoRepository.buscarPorId(new SessaoId(command.sessaoId()))
                .orElseThrow(() -> new SessaoNaoEncontradaException(command.sessaoId()));

        List<AssentoId> assentos = command.assentos().stream().map(AssentoId::new).toList();
        sessao.reservar(assentos);
        sessaoRepository.salvar(sessao);

        Reserva reserva = Reserva.criar(new ClienteId(command.clienteId()), sessao.getId(), assentos, sessao.getPreco().multiplicar(assentos.size()));
        reserva.aguardarPagamento();
        salvar(reserva);

        PagamentoGateway.PagamentoResultado pagamento = pagamentoGateway.cobrar(
                reserva.getId().valor(), reserva.getValorTotal().valor(), command.simularRecusaPagamento());

        if (!"APROVADO".equals(pagamento.status())) {
            log.warn("reserva.realizacao.falha reservaId={} motivo=pagamento_recusado", reserva.getId().valor());
            throw new PagamentoRecusadoException(reserva.getId().valor());
        }

        reserva.confirmarPagamento(pagamento.pagamentoId());
        reserva.iniciarEmissaoIngresso();
        salvar(reserva);

        try {
            IngressoGateway.IngressoResultado ingresso = ingressoGateway.emitir(
                    reserva.getId().valor(), reserva.getSessaoId().valor(),
                    reserva.getAssentos().stream().map(AssentoId::valor).toList(), command.simularFalhaIngresso());
            reserva.confirmar(ingresso.ingressoId());
            ReservaDetalhe detalhe = ReservaDetalhe.de(salvar(reserva));
            log.info("reserva.realizacao.sucesso reservaId={} status={} pagamentoId={} ingressoId={}",
                    detalhe.reservaId(), detalhe.status(), detalhe.pagamentoId(), detalhe.ingressoId());
            return detalhe;
        } catch (FalhaIntegracaoException ex) {
            // NÃO compensamos ainda. Essa lacuna é intencional: será resolvida quando ensinarmos Saga.
            log.error("reserva.realizacao.falha reservaId={} motivo=falha_emissao_ingresso", reserva.getId().valor());
            throw new FalhaProcessamentoReservaException(reserva.getId().valor(), pagamento.pagamentoId(), ex);
        }
    }
    @Transactional
    public ReservaDetalhe confirmarPagamento(UUID reservaId, UUID pagamentoId){
        Reserva reserva = reservaRepository
                .buscarPorId(new ReservaId(reservaId))
                .orElseThrow(
                        () -> new ReservaNaoEncontradaException(
                                reservaId
                        )
                );
        reserva.confirmarPagamento(pagamentoId);
        return ReservaDetalhe.de(salvar(reserva));
    }
    @Transactional
    public ReservaDetalhe cancelar(UUID reservaId){
        Reserva reserva = reservaRepository
                .buscarPorId(new ReservaId(reservaId))
                .orElseThrow(
                        () -> new ReservaNaoEncontradaException(
                                reservaId
                        )
                );
        Sessao sessao = sessaoRepository
                .buscarPorId(
                        reserva.getSessaoId()
                )
                .orElseThrow(
                        () -> new SessaoNaoEncontradaException(
                                reserva.getSessaoId().valor()
                        )
                );
        reserva.cancelar();
        sessao.liberar(reserva.getAssentos());
        sessaoRepository.salvar(sessao);
        salvar(reserva);
        return ReservaDetalhe.de(reserva);

    }
    @Transactional
    public ReservaDetalhe iniciarEmissaoIngresso(UUID reservaId){
        Reserva reserva = reservaRepository
                .buscarPorId(new ReservaId(reservaId))
                .orElseThrow(
                        () -> new ReservaNaoEncontradaException(
                                reservaId
                        )
                );
        reserva.iniciarEmissaoIngresso();
        return ReservaDetalhe.de(salvar(reserva));

    }
    @Transactional
    public ReservaDetalhe confirmar(UUID reservaId, UUID ingressoId){
        Reserva reserva = reservaRepository
                .buscarPorId(new ReservaId(reservaId))
                .orElseThrow(
                        () -> new ReservaNaoEncontradaException(
                                reservaId
                        )
                );
        reserva.confirmar(ingressoId);
        return ReservaDetalhe.de(salvar(reserva));

    }
    @Transactional
    public ReservaDetalhe iniciar(RealizarReservaCommand command) {
        Sessao sessao = sessaoRepository
                .buscarPorId(
                        new SessaoId(command.sessaoId())
                )
                .orElseThrow(
                        () -> new SessaoNaoEncontradaException(
                                command.sessaoId()
                        )
                );

        List<AssentoId> assentos =
                command.assentos()
                        .stream()
                        .map(AssentoId::new)
                        .toList();

        sessao.reservar(assentos);

        sessaoRepository.salvar(sessao);

        Reserva reserva = Reserva.criar(
                new ClienteId(command.clienteId()),
                sessao.getId(),
                assentos,
                sessao.getPreco()
                        .multiplicar(assentos.size())
        );

        reserva.aguardarPagamento();

        salvar(reserva);

        return ReservaDetalhe.de(reserva);
    }

    /**
     * Persiste o Aggregate e registra na outbox, na MESMA transação, os eventos
     * de domínio gerados pelas transições de estado (Transactional Outbox).
     */
    private Reserva salvar(Reserva reserva) {
        Reserva salva = reservaRepository.salvar(reserva);
        List<DomainEvent> pendentes = reserva.pullDomainEvents();
        if (!pendentes.isEmpty()) {
            eventos.registrar(pendentes, reserva.getId().valor());
        }
        MDC.put("reservaId", reserva.getId().valor().toString());
        log.info("reserva.persistida reservaId={} status={} eventos={}", reserva.getId().valor(), reserva.getStatus(), pendentes.size());
        return salva;
    }

    @Transactional(readOnly = true)
    public ReservaDetalhe buscar(UUID id) {
        return reservaRepository.buscarPorId(new ReservaId(id)).map(ReservaDetalhe::de)
                .orElseThrow(() -> new ReservaNaoEncontradaException(id));
    }

    @Transactional(readOnly = true)
    public List<ReservaDetalhe> listar() { return reservaRepository.listar().stream().map(ReservaDetalhe::de).toList(); }
}

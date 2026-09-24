package br.com.cinepass.notificacao.application;

import br.com.cinepass.notificacao.application.port.EventosProcessados;
import br.com.cinepass.notificacao.domain.model.Notificacao;
import br.com.cinepass.notificacao.domain.model.TipoNotificacao;
import br.com.cinepass.notificacao.domain.repository.NotificacaoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class NotificacaoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoApplicationService.class);

    private final NotificacaoRepository notificacoes;
    private final EventosProcessados eventosProcessados;

    public NotificacaoApplicationService(NotificacaoRepository notificacoes, EventosProcessados eventosProcessados) {
        this.notificacoes = notificacoes;
        this.eventosProcessados = eventosProcessados;
    }

    /**
     * Idempotente: se o eventId já foi processado, nada é feito (nenhuma
     * notificação repetida). A notificação e o registro do eventId são
     * gravados na mesma transação.
     */
    @Transactional
    public void processar(EventoReservaRecebido evento) {
        if (eventosProcessados.jaProcessado(evento.eventId())) {
            log.info("notificacao.evento.duplicado.ignorado eventId={} eventType={} reservaId={}",
                    evento.eventId(), evento.eventType(), evento.reservaId());
            return;
        }
        TipoNotificacao tipo;
        String mensagem;
        switch (evento.eventType()) {
            case "ReservaCriada" -> {
                tipo = TipoNotificacao.RESERVA_CRIADA;
                mensagem = "Recebemos sua reserva dos assentos " + evento.assentos() + ". Aguardando o pagamento.";
            }
            case "ReservaPagamentoAprovado" -> {
                tipo = TipoNotificacao.PAGAMENTO_APROVADO;
                mensagem = "O pagamento de R$ " + evento.valorTotal() + " foi aprovado. Estamos emitindo seu ingresso.";
            }
            case "ReservaConfirmada" -> {
                tipo = TipoNotificacao.RESERVA_CONFIRMADA;
                mensagem = "Reserva confirmada! Seu ingresso para os assentos " + evento.assentos() + " foi emitido.";
            }
            case "ReservaCancelada" -> {
                tipo = TipoNotificacao.RESERVA_CANCELADA;
                mensagem = "Sua reserva foi cancelada e os assentos foram liberados.";
            }
            default -> {
                log.info("notificacao.evento.ignorado eventId={} eventType={} motivo=tipo_nao_tratado", evento.eventId(), evento.eventType());
                return;
            }
        }
        Notificacao notificacao = notificacoes.salvar(
                Notificacao.criar(evento.reservaId(), evento.clienteId(), tipo, mensagem, evento.eventId()));
        eventosProcessados.registrar(evento.eventId(), evento.eventType());
        log.info("notificacao.evento.processado.sucesso eventId={} eventType={} reservaId={} destinatarioId={} notificacaoId={} resultado=notificacao_registrada",
                evento.eventId(), evento.eventType(), evento.reservaId(), evento.clienteId(), notificacao.getId());
    }

    @Transactional(readOnly = true)
    public List<Notificacao> listar() { return notificacoes.listar(); }

    @Transactional(readOnly = true)
    public List<Notificacao> listarPorReserva(UUID reservaId) { return notificacoes.listarPorReserva(reservaId); }
}

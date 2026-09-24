package br.com.cinepass.notificacao.presentation;

import br.com.cinepass.notificacao.application.NotificacaoApplicationService;
import br.com.cinepass.notificacao.domain.model.Notificacao;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/notificacoes")
public class NotificacaoController {
    private final NotificacaoApplicationService service;

    public NotificacaoController(NotificacaoApplicationService service) { this.service = service; }

    @GetMapping
    public List<NotificacaoResponse> listar() { return service.listar().stream().map(NotificacaoResponse::de).toList(); }

    @GetMapping("/reserva/{reservaId}")
    public List<NotificacaoResponse> porReserva(@PathVariable UUID reservaId) {
        return service.listarPorReserva(reservaId).stream().map(NotificacaoResponse::de).toList();
    }

    public record NotificacaoResponse(UUID notificacaoId, UUID reservaId, UUID destinatarioId, String tipo, String mensagem,
                                      UUID eventId, Instant criadaEm) {
        static NotificacaoResponse de(Notificacao n) {
            return new NotificacaoResponse(n.getId(), n.getReservaId(), n.getDestinatarioId(), n.getTipo().name(),
                    n.getMensagem(), n.getEventId(), n.getCriadaEm());
        }
    }
}

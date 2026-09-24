package br.com.cinepass.auditoria.presentation;

import br.com.cinepass.auditoria.application.AuditoriaApplicationService;
import br.com.cinepass.auditoria.domain.model.RegistroAuditoria;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {
    private final AuditoriaApplicationService service;

    public AuditoriaController(AuditoriaApplicationService service) { this.service = service; }

    @GetMapping
    public List<AuditoriaResponse> listar() { return service.listar().stream().map(AuditoriaResponse::de).toList(); }

    @GetMapping("/reserva/{reservaId}")
    public List<AuditoriaResponse> porReserva(@PathVariable UUID reservaId) {
        return service.listarPorReserva(reservaId).stream().map(AuditoriaResponse::de).toList();
    }

    public record AuditoriaResponse(UUID eventId, String eventType, UUID reservaId, String correlationId, Instant occurredAt,
                                    Instant recebidoEm, int particao, long offset, String payload) {
        static AuditoriaResponse de(RegistroAuditoria r) {
            return new AuditoriaResponse(r.getEventId(), r.getEventType(), r.getReservaId(), r.getCorrelationId(),
                    r.getOccurredAt(), r.getRecebidoEm(), r.getParticao(), r.getOffset(), r.getPayload());
        }
    }
}

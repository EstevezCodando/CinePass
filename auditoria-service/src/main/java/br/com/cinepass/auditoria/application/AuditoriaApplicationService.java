package br.com.cinepass.auditoria.application;

import br.com.cinepass.auditoria.domain.model.RegistroAuditoria;
import br.com.cinepass.auditoria.domain.repository.RegistroAuditoriaRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class AuditoriaApplicationService {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaApplicationService.class);

    private final RegistroAuditoriaRepository registros;

    public AuditoriaApplicationService(RegistroAuditoriaRepository registros) { this.registros = registros; }

    /**
     * Registra todos os eventos (fan-in). A coluna event_id é única e funciona
     * como guarda de idempotência: um evento reentregue não gera outro registro.
     */
    @Transactional
    public void registrar(EventoParaAuditoria evento) {
        if (registros.existePorEventId(evento.eventId())) {
            log.info("auditoria.evento.duplicado.ignorado eventId={} eventType={} reservaId={}",
                    evento.eventId(), evento.eventType(), evento.reservaId());
            return;
        }
        RegistroAuditoria registro = registros.salvar(RegistroAuditoria.registrar(evento.eventId(), evento.reservaId(),
                evento.eventType(), evento.correlationId(), evento.payload(), evento.occurredAt(), evento.particao(), evento.offset()));
        log.info("auditoria.evento.processado.sucesso auditoriaId={} eventId={} eventType={} reservaId={} particao={} offset={} resultado=auditoria_registrada",
                registro.getId(), evento.eventId(), evento.eventType(), evento.reservaId(), evento.particao(), evento.offset());
    }

    @Transactional(readOnly = true)
    public List<RegistroAuditoria> listar() { return registros.listar(); }

    @Transactional(readOnly = true)
    public List<RegistroAuditoria> listarPorReserva(UUID reservaId) { return registros.listarPorReserva(reservaId); }
}

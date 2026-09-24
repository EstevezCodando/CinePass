package br.com.cinepass.auditoria.domain.repository;

import br.com.cinepass.auditoria.domain.model.RegistroAuditoria;

import java.util.List;
import java.util.UUID;

public interface RegistroAuditoriaRepository {
    RegistroAuditoria salvar(RegistroAuditoria registro);
    boolean existePorEventId(UUID eventId);
    List<RegistroAuditoria> listar();
    List<RegistroAuditoria> listarPorReserva(UUID reservaId);
}

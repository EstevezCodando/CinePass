package br.com.cinepass.auditoria.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

interface SpringDataRegistroAuditoriaRepository extends JpaRepository<RegistroAuditoriaJpaEntity, UUID> {
    boolean existsByEventId(UUID eventId);
    List<RegistroAuditoriaJpaEntity> findByReservaIdOrderByRecebidoEmAsc(UUID reservaId);
}

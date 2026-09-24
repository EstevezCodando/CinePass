package br.com.cinepass.reserva.infrastructure.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

interface SpringDataOutboxRepository extends JpaRepository<OutboxEventJpaEntity, UUID> {

    /**
     * Advisory lock transacional do PostgreSQL: só uma instância do
     * reserva-service publica a outbox por vez (evita que duas instâncias
     * publiquem eventos da mesma reserva em paralelo e invertam a ordem).
     */
    @Query(value = "SELECT pg_try_advisory_xact_lock(:chave)", nativeQuery = true)
    boolean tentarLockDoPublicador(@Param("chave") long chave);

    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDENTE'
            ORDER BY created_at ASC
            LIMIT :limite
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventJpaEntity> buscarLotePendente(@Param("limite") int limite);
}

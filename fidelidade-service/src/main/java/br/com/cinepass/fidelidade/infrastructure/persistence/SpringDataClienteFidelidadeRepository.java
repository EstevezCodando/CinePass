package br.com.cinepass.fidelidade.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

interface SpringDataClienteFidelidadeRepository extends JpaRepository<ClienteFidelidadeJpaEntity, UUID> {}

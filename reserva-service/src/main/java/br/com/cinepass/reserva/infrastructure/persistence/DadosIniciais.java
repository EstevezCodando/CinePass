package br.com.cinepass.reserva.infrastructure.persistence;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.UUID;

@Component
public class DadosIniciais implements CommandLineRunner {
    public static final UUID FILME_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    public static final UUID SESSAO_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private final SpringDataFilmeRepository filmes;
    private final SpringDataSessaoRepository sessoes;
    public DadosIniciais(SpringDataFilmeRepository filmes, SpringDataSessaoRepository sessoes){this.filmes=filmes;this.sessoes=sessoes;}

    @Override @Transactional
    public void run(String... args){
        if (!filmes.existsById(FILME_ID)) filmes.save(new FilmeJpaEntity(FILME_ID, "Arquitetos do Amanhã", 128, "12"));
        if (!sessoes.existsById(SESSAO_ID)) {
            sessoes.save(new SessaoJpaEntity(SESSAO_ID, FILME_ID, "Sala 1", LocalDateTime.now().plusDays(1).withMinute(0).withSecond(0).withNano(0),
                    new BigDecimal("39.90"), Set.of("A1","A2","A3","A4","A5","B1","B2","B3","B4","B5")));
        }
    }
}

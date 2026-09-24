package br.com.cinepass.fidelidade.application;

import br.com.cinepass.fidelidade.application.port.EventosProcessados;
import br.com.cinepass.fidelidade.domain.model.ClienteFidelidade;
import br.com.cinepass.fidelidade.domain.repository.ClienteFidelidadeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class FidelidadeApplicationService {
    private static final Logger log = LoggerFactory.getLogger(FidelidadeApplicationService.class);

    private final ClienteFidelidadeRepository clientes;
    private final EventosProcessados eventosProcessados;

    public FidelidadeApplicationService(ClienteFidelidadeRepository clientes, EventosProcessados eventosProcessados) {
        this.clientes = clientes;
        this.eventosProcessados = eventosProcessados;
    }

    /**
     * Idempotente. É aqui que a idempotência mais importa: a operação é
     * aditiva (soma pontos e valor), então reaplicar o mesmo evento contaria a
     * reserva duas vezes. O histórico e o eventId são gravados juntos.
     */
    @Transactional
    public void registrarReservaConfirmada(ReservaConfirmadaRecebida evento) {
        if (eventosProcessados.jaProcessado(evento.eventId())) {
            log.info("fidelidade.evento.duplicado.ignorado eventId={} eventType={} reservaId={}",
                    evento.eventId(), evento.eventType(), evento.reservaId());
            return;
        }
        ClienteFidelidade cliente = clientes.buscarPorId(evento.clienteId())
                .orElseGet(() -> ClienteFidelidade.novo(evento.clienteId()));
        cliente.registrarReservaConfirmada(evento.ingressos(), evento.valorTotal());
        clientes.salvar(cliente);
        eventosProcessados.registrar(evento.eventId(), evento.eventType());
        log.info("fidelidade.evento.processado.sucesso eventId={} reservaId={} clienteId={} reservasConfirmadas={} pontos={} resultado=historico_atualizado",
                evento.eventId(), evento.reservaId(), evento.clienteId(), cliente.getReservasConfirmadas(), cliente.getPontos());
    }

    @Transactional(readOnly = true)
    public ClienteFidelidade buscar(UUID clienteId) {
        return clientes.buscarPorId(clienteId).orElseThrow(() -> new ClienteNaoEncontradoException(clienteId));
    }

    @Transactional(readOnly = true)
    public List<ClienteFidelidade> listar() { return clientes.listar(); }
}

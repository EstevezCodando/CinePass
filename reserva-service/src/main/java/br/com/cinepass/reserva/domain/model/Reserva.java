package br.com.cinepass.reserva.domain.model;

import br.com.cinepass.reserva.domain.event.ReservaCancelada;
import br.com.cinepass.reserva.domain.event.ReservaConfirmada;
import br.com.cinepass.reserva.domain.event.ReservaCriada;
import br.com.cinepass.reserva.domain.event.ReservaPagamentoAprovado;
import br.com.cinepass.reserva.domain.shared.DomainEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class Reserva {
    private final ReservaId id;
    private final ClienteId clienteId;
    private final SessaoId sessaoId;
    private final List<AssentoId> assentos;
    private final Dinheiro valorTotal;
    private StatusReserva status;
    private UUID pagamentoId;
    private UUID ingressoId;
    private final Instant criadaEm;
    private final List<DomainEvent> domainEvents = new ArrayList<>();

    private Reserva(ReservaId id, ClienteId clienteId, SessaoId sessaoId, List<AssentoId> assentos, Dinheiro valorTotal,
                    StatusReserva status, UUID pagamentoId, UUID ingressoId, Instant criadaEm) {
        this.id=Objects.requireNonNull(id); this.clienteId=Objects.requireNonNull(clienteId); this.sessaoId=Objects.requireNonNull(sessaoId);
        this.assentos=List.copyOf(assentos); if (this.assentos.isEmpty()) throw new IllegalArgumentException("A reserva precisa de assentos.");
        this.valorTotal=Objects.requireNonNull(valorTotal); this.status=Objects.requireNonNull(status);
        this.pagamentoId=pagamentoId; this.ingressoId=ingressoId; this.criadaEm=Objects.requireNonNull(criadaEm);
    }

    public static Reserva criar(ClienteId clienteId, SessaoId sessaoId, List<AssentoId> assentos, Dinheiro valorTotal) {
        Reserva reserva = new Reserva(ReservaId.novo(), clienteId, sessaoId, assentos, valorTotal, StatusReserva.CRIADA, null, null, Instant.now());
        reserva.domainEvents.add(ReservaCriada.de(reserva));
        return reserva;
    }

    public static Reserva restaurar(ReservaId id, ClienteId clienteId, SessaoId sessaoId, List<AssentoId> assentos, Dinheiro valorTotal,
                                    StatusReserva status, UUID pagamentoId, UUID ingressoId, Instant criadaEm) {
        return new Reserva(id, clienteId, sessaoId, assentos, valorTotal, status, pagamentoId, ingressoId, criadaEm);
    }

    public void aguardarPagamento() { exigir(StatusReserva.CRIADA); status = StatusReserva.AGUARDANDO_PAGAMENTO; }
    public void confirmarPagamento(UUID pagamentoId) {
        exigir(StatusReserva.AGUARDANDO_PAGAMENTO); this.pagamentoId = Objects.requireNonNull(pagamentoId); status = StatusReserva.PAGAMENTO_APROVADO;
        domainEvents.add(ReservaPagamentoAprovado.de(this));
    }
    public void iniciarEmissaoIngresso() { exigir(StatusReserva.PAGAMENTO_APROVADO); status = StatusReserva.EMITINDO_INGRESSO; }
    public void confirmar(UUID ingressoId) {
        exigir(StatusReserva.EMITINDO_INGRESSO); this.ingressoId = Objects.requireNonNull(ingressoId); status = StatusReserva.CONFIRMADA;
        domainEvents.add(ReservaConfirmada.de(this));
    }
    public void cancelar() {
        if (status == StatusReserva.CANCELADA) return;
        StatusReserva anterior = status;
        status = StatusReserva.CANCELADA;
        domainEvents.add(ReservaCancelada.de(this, anterior.name()));
    }
    /** Devolve e limpa os eventos de domínio gerados desde a última chamada. */
    public List<DomainEvent> pullDomainEvents() {
        List<DomainEvent> eventos = List.copyOf(domainEvents);
        domainEvents.clear();
        return eventos;
    }
    private void exigir(StatusReserva esperado) { if (status != esperado) throw new IllegalStateException("Estado inválido. Esperado: " + esperado + ", atual: " + status); }

    public ReservaId getId(){return id;} public ClienteId getClienteId(){return clienteId;} public SessaoId getSessaoId(){return sessaoId;}
    public List<AssentoId> getAssentos(){return assentos;} public Dinheiro getValorTotal(){return valorTotal;} public StatusReserva getStatus(){return status;}
    public UUID getPagamentoId(){return pagamentoId;} public UUID getIngressoId(){return ingressoId;} public Instant getCriadaEm(){return criadaEm;}
}

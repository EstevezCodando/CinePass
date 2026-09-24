package br.com.cinepass.fidelidade.presentation;

import br.com.cinepass.fidelidade.application.ClienteNaoEncontradoException;
import br.com.cinepass.fidelidade.application.FidelidadeApplicationService;
import br.com.cinepass.fidelidade.domain.model.ClienteFidelidade;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/fidelidade")
public class FidelidadeController {
    private final FidelidadeApplicationService service;

    public FidelidadeController(FidelidadeApplicationService service) { this.service = service; }

    @GetMapping
    public List<FidelidadeResponse> listar() { return service.listar().stream().map(FidelidadeResponse::de).toList(); }

    @GetMapping("/{clienteId}")
    public FidelidadeResponse buscar(@PathVariable UUID clienteId) { return FidelidadeResponse.de(service.buscar(clienteId)); }

    @ExceptionHandler(ClienteNaoEncontradoException.class)
    ProblemDetail naoEncontrado(ClienteNaoEncontradoException ex) {
        var p = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        p.setTitle("Cliente não encontrado");
        return p;
    }

    public record FidelidadeResponse(UUID clienteId, int reservasConfirmadas, int ingressosComprados,
                                     BigDecimal valorTotalGasto, long pontos) {
        static FidelidadeResponse de(ClienteFidelidade c) {
            return new FidelidadeResponse(c.getClienteId(), c.getReservasConfirmadas(), c.getIngressosComprados(),
                    c.getValorTotalGasto(), c.getPontos());
        }
    }
}

package br.com.cinepass.reserva.presentation;

import br.com.cinepass.reserva.application.*;
import br.com.cinepass.reserva.domain.model.AssentoIndisponivelException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler({ReservaNaoEncontradaException.class, SessaoNaoEncontradaException.class})
    ProblemDetail naoEncontrado(RuntimeException ex){
        var p=ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage()); p.setTitle("Recurso não encontrado"); return p;
    }

    @ExceptionHandler({AssentoIndisponivelException.class, IllegalArgumentException.class, IllegalStateException.class})
    ProblemDetail negocio(RuntimeException ex){
        var p=ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage()); p.setTitle("Regra de negócio violada"); return p;
    }

    @ExceptionHandler(PagamentoRecusadoException.class)
    ProblemDetail pagamentoRecusado(PagamentoRecusadoException ex){
        var p=ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
        p.setTitle("Pagamento recusado"); p.setProperty("reservaId", ex.getReservaId()); return p;
    }

    @ExceptionHandler(FalhaProcessamentoReservaException.class)
    ProblemDetail falhaDistribuida(FalhaProcessamentoReservaException ex){
        var p=ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
        p.setTitle("Inconsistência distribuída proposital");
        p.setProperty("reservaId", ex.getReservaId());
        p.setProperty("pagamentoId", ex.getPagamentoId());
        p.setProperty("proximoPassoDaAula", "Implementar compensação com Saga Pattern");
        return p;
    }

    @ExceptionHandler(FalhaIntegracaoException.class)
    ProblemDetail integracao(FalhaIntegracaoException ex){
        var p=ProblemDetail.forStatusAndDetail(HttpStatus.BAD_GATEWAY, ex.getMessage()); p.setTitle("Falha de integração"); return p;
    }
}

package br.com.cinepass.fidelidade.application;

import java.util.UUID;

public class ClienteNaoEncontradoException extends RuntimeException {
    public ClienteNaoEncontradoException(UUID clienteId) { super("Cliente sem histórico de fidelidade: " + clienteId); }
}

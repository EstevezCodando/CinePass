# Observabilidade

Logs padronizados e centralizados, correlação das operações e rastreamento
distribuído em todos os microsserviços do CinePass.

## 1. Logs

Cada serviço tem um `logback-spring.xml` com dois destinos:

- **Console**: formato textual, com `service`, `traceId`, `spanId`, `correlationId` e, quando aplicável, `reservaId`/`eventId`.
- **Logstash** (`LogstashTcpSocketAppender`, assíncrono e com `neverBlock`): cada linha vai em JSON para o ELK. Se o Logstash cair, o serviço continua funcionando normalmente.

```text
2026-09-23 14:05:47.012 INFO  service=reserva-service traceId=6ab3... spanId=1b2c... correlationId=teste-001 reservaId=8f3a... thread=http-nio-8081-exec-1 logger=b.c.c.r.a.ReservaApplicationService - reserva.realizacao.sucesso ...
```

Os pontos de log seguem um padrão de nomes, identificando início, conclusão,
falhas, eventos publicados e eventos recebidos:

| Serviço | Pontos de log |
|---|---|
| api-gateway | `gateway.request.inicio`, `gateway.request.fim` |
| reserva-service | `reserva.realizacao.inicio/sucesso/falha`, `reserva.persistida`, `reserva.outbox.registrado`, `reserva.outbox.publicado`, `reserva.outbox.falha` |
| pagamento-service | `pagamento.processamento.inicio/fim`, `pagamento.estorno` |
| ingresso-service | `ingresso.emissao.inicio/fim/falha`, `ingresso.cancelamento` |
| notificacao-service | `kafka.evento.recebido`, `notificacao.evento.processado.sucesso`, `notificacao.evento.duplicado.ignorado`, `kafka.evento.falha/retry/dlt.encaminhado` |
| fidelidade-service | `kafka.evento.recebido`, `fidelidade.evento.processado.sucesso`, `fidelidade.evento.duplicado.ignorado`, `fidelidade.evento.ignorado`, `kafka.evento.falha/retry/dlt.encaminhado` |
| auditoria-service | `kafka.evento.recebido`, `auditoria.evento.processado.sucesso`, `auditoria.evento.duplicado.ignorado`, `kafka.evento.falha/retry/dlt.encaminhado` |

Não são registrados dados sensíveis (senhas, tokens, dados de cartão). Os
identificadores que aparecem são UUIDs de domínio e o `correlationId`.

## 2. Correlação (`correlationId`)

```text
Cliente → API Gateway → reserva-service → (HTTP) pagamento / ingresso
                               └→ outbox → Kafka (header + payload) → notificação / fidelidade / auditoria
```

1. **API Gateway** (`CorrelationIdGatewayFilter`): usa o `X-Correlation-Id` do cliente ou gera um UUID, repassa ao serviço de destino e devolve na resposta. Como o Gateway é reativo, o valor vai no log como campo estruturado (`StructuredArguments`).
2. **reserva-service** (`CorrelationIdFilter`): coloca o valor no MDC (ou gera um, se a chamada não passou pelo Gateway). No fluxo `/api/reservas/temporal`, o `CorrelationIdContextPropagator` leva o valor do MDC para o workflow e para as activities do Temporal, que rodam em outras threads. O `OutboxEventWriter` o grava no envelope do evento, e o `HttpClientConfig` o repassa nas chamadas HTTP internas para o pagamento-service e o ingresso-service.
3. **pagamento-service / ingresso-service**: também têm um `CorrelationIdFilter`, então seus logs aparecem na mesma busca.
4. **Kafka**: o `correlationId` vai no payload e no header.
5. **Consumidores**: cada listener recoloca `correlationId`, `reservaId`, `eventId` e `eventType` no MDC antes de processar.

## 3. Centralização de logs (ELK)

- **Logstash** (`:5000`, TCP) recebe o JSON e grava em `cinepass-logs-AAAA.MM.dd` (`infra/logstash/pipeline/logstash.conf`).
- **Elasticsearch** (`:9200`) armazena os logs.
- **Kibana** (`http://localhost:5601`): crie o data view uma vez, com `./infra/scripts/kibana-setup.sh`.

Para acompanhar uma operação sem abrir o console de nenhum serviço, use no
Discover `correlationId.keyword : "<valor>"`. O sufixo `.keyword` faz a busca
exata. Também é possível filtrar por `reservaId.keyword` e `eventId.keyword`.

## 4. Rastreamento distribuído (Zipkin)

O Spring Cloud Sleuth citado no enunciado foi descontinuado a partir do Spring
Boot 3; o substituto oficial é o **Micrometer Tracing**, usado aqui com Brave e
exportação para o Zipkin. No Spring Boot 4 a autoconfiguração fica em módulos
próprios:

```xml
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-micrometer-tracing-brave</artifactId></dependency>
<dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-zipkin</artifactId></dependency>
```

```yaml
management:
  tracing:
    sampling:
      probability: 1.0      # 100% das requisições (ambiente de demonstração)
    export:
      zipkin:
        endpoint: http://zipkin:9411/api/v2/spans
```

Três ajustes foram necessários para o trace atravessar tudo:

- **HTTP interno**: os `RestClient.Builder` do CinePass são criados à mão (`RestClient.builder()`) e não recebiam o `ObservationRegistry`. O `HttpClientConfig` passou a configurá-lo, e o trace segue do reserva-service para o pagamento-service e o ingresso-service.
- **Outbox**: a publicação acontece depois, em outra thread. O `TraceContextOutbox` grava o contexto de trace na linha da outbox e recria o span na hora de publicar.
- **Kafka**: `spring.kafka.template.observation-enabled: true` no produtor injeta o header `traceparent`. Nos consumidores, a factory do listener é própria, então a observação é ligada nela (`setObservationEnabled(true)`).

O resultado no Zipkin (`http://localhost:9411`) é um trace único:

```text
api-gateway → reserva-service → pagamento-service
                              → ingresso-service
                              → outbox → Kafka → notificacao-service
                                               → fidelidade-service
                                               → auditoria-service
```

O `discovery-server` envia logs ao ELK, mas não participa do tracing, por não
fazer parte do fluxo de negócio.

## 5. Falhas e reprocessamento

Descrito em `docs/EVENTS.md` (seção "Falhas e dead-letter"). Resumo:

- 4 tentativas por mensagem (1 + 3 retentativas, com 1 s de intervalo);
- depois, envio ao DLT do serviço, com a causa nos headers;
- as outras partições seguem sendo consumidas normalmente;
- reprocessamento com `./infra/scripts/reprocessar-dlt.sh`.

Nos consumidores, o `connection-timeout` do Hikari é de 3 s: se o banco cair,
a mensagem vai rapidamente para retry/DLT, em vez de prender a thread.

# Evidências de execução

Evidências coletadas com o ambiente completo em execução, recriado do zero
(`docker compose down -v` e `docker compose up --build -d`, na raiz do
projeto). Todas as saídas em texto foram geradas pelo script
**`infra/scripts/coletar-evidencias.sh`**, que pode ser executado de novo a
qualquer momento (em um ambiente recém-criado, porque os cenários usam assentos
fixos da sessão de exemplo). As saídas completas ficam em
[`docs/evidencias/`](evidencias/); abaixo estão as mesmas saídas, com uma
explicação de cada cenário.

Operação usada como fio condutor:

```text
correlationId = evidencia-20260923155207
reservaId     = 893a19ad-9598-4083-9bb2-4029c44516b9
traceId       = 6ab41fd89de166800237a007687112d3
```

Arquitetura: [`ARQUITETURA.md`](ARQUITETURA.md) · Eventos: [`EVENTS.md`](EVENTS.md) · Observabilidade: [`OBSERVABILIDADE.md`](OBSERVABILIDADE.md)

![Eureka: serviços registrados](evidencias/eureka.png)

## 0. Ambiente

Os 17 containers do `docker-compose.yml` no ar e os 7 serviços registrados no Eureka.

```text
NAMES                    STATUS
cinepass-gateway         Up 2 minutes
cinepass-notificacao     Up 2 minutes
cinepass-fidelidade      Up 2 minutes
cinepass-auditoria       Up 2 minutes
cinepass-reserva         Up 2 minutes
cinepass-temporal-ui     Up 3 minutes
cinepass-temporal        Up 3 minutes
cinepass-ingresso        Up 3 minutes
cinepass-kafka-ui        Up 2 minutes
cinepass-pagamento       Up 3 minutes
cinepass-logstash        Up About a minute
cinepass-kibana          Up About a minute
cinepass-postgres        Up 3 minutes (healthy)
cinepass-kafka           Up 3 minutes (healthy)
cinepass-discovery       Up 3 minutes
cinepass-elasticsearch   Up 3 minutes (healthy)
cinepass-zipkin          Up 3 minutes (healthy)

"name":"API-GATEWAY"
"name":"AUDITORIA-SERVICE"
"name":"FIDELIDADE-SERVICE"
"name":"INGRESSO-SERVICE"
"name":"NOTIFICACAO-SERVICE"
"name":"PAGAMENTO-SERVICE"
"name":"RESERVA-SERVICE"
```

## 1. Requisição recebida pelo API Gateway

Reserva dos assentos A1 e A2 pelo Gateway (`:8080`), com `X-Correlation-Id` informado pelo cliente. O Gateway devolve o mesmo identificador na resposta e registra o início e o fim da requisição com o `traceId`. A reserva termina `CONFIRMADA` (fluxo síncrono: pagamento e ingresso chamados por HTTP).

```text
$ curl -i -X POST http://localhost:8080/api/reservas -H 'X-Correlation-Id: evidencia-20260923155207' -d '{... "assentos":["A1","A2"] ...}'
HTTP/1.1 201 Created
Content-Type: application/json
Content-Length: 366
Date: Wed, 23 Sep 2026 18:52:09 GMT
X-Correlation-Id: evidencia-20260923155207

{"reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","clienteId":"33333333-3333-3333-3333-155207000000","sessaoId":"22222222-2222-2222-2222-222222222222","assentos":["A1","A2"],"valorTotal":79.80,"status":"CONFIRMADA","pagamentoId":"ea2d6c35-e253-46bd-875b-c87f3cdb7fc2","ingressoId":"69a714a0-cbe1-4c73-9e99-403840c5f17b","criadaEm":"2026-09-23T18:52:08.354888881Z"}

--- logs do api-gateway para este correlationId ---
2026-09-23 18:52:08.233 INFO  service=api-gateway traceId=6ab41fd89de166800237a007687112d3 spanId=0237a007687112d3 correlationId= thread=reactor-http-epoll-2 logger=b.c.c.g.CorrelationIdGatewayFilter - gateway.request.inicio method=POST path=/api/reservas correlationId=evidencia-20260923155207
2026-09-23 18:52:09.923 INFO  service=api-gateway traceId=6ab41fd89de166800237a007687112d3 spanId=0237a007687112d3 correlationId= thread=reactor-http-epoll-2 logger=b.c.c.g.CorrelationIdGatewayFilter - gateway.request.fim method=POST path=/api/reservas status=201 CREATED durationMs=1690 correlationId=evidencia-20260923155207
```

## 2. Alteração persistida no reserva-service (e consistência no rollback)

A reserva foi gravada no `cinepass_db` e os três eventos de domínio foram gravados em `outbox_events` **na mesma transação** e depois publicados (`PUBLICADO`, com `published_at`). O cenário 2b faz uma reserva com `simularFalhaIngresso=true`: a transação local faz rollback e, junto com a reserva, somem os eventos da outbox. Nenhum consumidor é avisado de uma reserva que não existe.

```text
                  id                  |   status   | valor_total |             pagamento_id             |             ingresso_id              |           criada_em
--------------------------------------+------------+-------------+--------------------------------------+--------------------------------------+-------------------------------
 893a19ad-9598-4083-9bb2-4029c44516b9 | CONFIRMADA |       79.80 | ea2d6c35-e253-46bd-875b-c87f3cdb7fc2 | 69a714a0-cbe1-4c73-9e99-403840c5f17b | 2026-09-23 18:52:08.354889+00
(1 row)

--- outbox: eventos gravados na mesma transação da reserva e depois publicados ---
        event_type        |  status   |          created_at           |         published_at          |      correlation_id
--------------------------+-----------+-------------------------------+-------------------------------+--------------------------
 ReservaCriada            | PUBLICADO | 2026-09-23 18:52:08.383044+00 | 2026-09-23 18:52:10.47849+00  | evidencia-20260923155207
 ReservaPagamentoAprovado | PUBLICADO | 2026-09-23 18:52:09.203083+00 | 2026-09-23 18:52:10.506991+00 | evidencia-20260923155207
 ReservaConfirmada        | PUBLICADO | 2026-09-23 18:52:09.856869+00 | 2026-09-23 18:52:10.527317+00 | evidencia-20260923155207
(3 rows)

--- logs do reserva-service ---
2026-09-23 18:52:08.388 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId= thread=http-nio-8081-exec-2 logger=b.c.c.r.i.outbox.OutboxEventWriter - reserva.outbox.registrado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada correlationId=evidencia-20260923155207
2026-09-23 18:52:08.388 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.a.ReservaApplicationService - reserva.persistida reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=AGUARDANDO_PAGAMENTO eventos=1
2026-09-23 18:52:09.205 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.i.outbox.OutboxEventWriter - reserva.outbox.registrado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado correlationId=evidencia-20260923155207
2026-09-23 18:52:09.205 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.a.ReservaApplicationService - reserva.persistida reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=EMITINDO_INGRESSO eventos=1
2026-09-23 18:52:09.862 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.i.outbox.OutboxEventWriter - reserva.outbox.registrado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada correlationId=evidencia-20260923155207
2026-09-23 18:52:09.863 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.a.ReservaApplicationService - reserva.persistida reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=CONFIRMADA eventos=1
2026-09-23 18:52:09.865 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.a.ReservaApplicationService - reserva.realizacao.sucesso reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=CONFIRMADA pagamentoId=ea2d6c35-e253-46bd-875b-c87f3cdb7fc2 ingressoId=69a714a0-cbe1-4c73-9e99-403840c5f17b

==================================================================
2b. CONSISTÊNCIA: reserva que falha na emissão do ingresso não deixa evento na outbox
==================================================================
HTTP/1.1 500 Internal Server Error
{"detail":"O pagamento foi aprovado, mas a emissão do ingresso falhou. A transação local será revertida, porém o pagamento remoto continuará confirmado.","instance":"/api/reservas","status":500,"title":"Inconsistência distribuída proposital","reservaId":"1afc7744-869e-43ca-9f99-1fdfa958431f","pagamentoId":"e0eee587-aad1-49d9-a167-852cf34768c9","proximoPassoDaAula":"Implementar compensação com Saga Pattern"}

 reservas_com_esse_id
----------------------
                    0
(1 row)

 eventos_na_outbox
-------------------
                 0
(1 row)
```

## 3. Eventos publicados no Apache Kafka

O `OutboxPublisher` publicou os três eventos no tópico `reserva.eventos` com a chave `reservaId`: mesma partição, offsets crescentes. As mensagens lidas do tópico mostram os headers `eventId`, `eventType`, `correlationId` e `traceparent`.

```text
2026-09-23 18:52:10.478 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=8f92d2ee6a2cd7d5 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=scheduling-1 logger=b.c.c.r.i.outbox.OutboxPublisher - reserva.outbox.publicado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada topic=reserva.eventos partition=0 offset=0
2026-09-23 18:52:10.507 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=029e41fe9d7ccb68 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=scheduling-1 logger=b.c.c.r.i.outbox.OutboxPublisher - reserva.outbox.publicado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado topic=reserva.eventos partition=0 offset=1
2026-09-23 18:52:10.527 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=b51c8cb274a3b210 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=scheduling-1 logger=b.c.c.r.i.outbox.OutboxPublisher - reserva.outbox.publicado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada topic=reserva.eventos partition=0 offset=2

--- mensagens lidas do tópico (partição, offset, headers, chave, valor) ---
Partition:0	Offset:0	eventId:33bfceaa-1ff5-40f3-b700-45a28d1247f8,eventType:ReservaCriada,correlationId:evidencia-20260923155207,traceparent:00-6ab41fd89de166800237a007687112d3-a172dc2828015cf5-01	893a19ad-9598-4083-9bb2-4029c44516b9	{"eventId":"33bfceaa-1ff5-40f3-b700-45a28d1247f8","eventType":"ReservaCriada","eventVersion":1,"occurredAt":"2026-09-23T18:52:08.355273720Z","reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","correlationId":"evidencia-20260923155207","producer":"reserva-service","data":{"reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","clienteId":"33333333-3333-3333-3333-155207000000","sessaoId":"22222222-2222-2222-2222-222222222222","assentos":["A1","A2"],"valorTotal":79.80,"status":"AGUARDANDO_PAGAMENTO"}}
Partition:0	Offset:1	eventId:3ec38043-0349-4442-991c-b56de1ba9cc5,eventType:ReservaPagamentoAprovado,correlationId:evidencia-20260923155207,traceparent:00-6ab41fd89de166800237a007687112d3-60105387030c1e1f-01	893a19ad-9598-4083-9bb2-4029c44516b9	{"eventId":"3ec38043-0349-4442-991c-b56de1ba9cc5","eventType":"ReservaPagamentoAprovado","eventVersion":1,"occurredAt":"2026-09-23T18:52:09.200135803Z","reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","correlationId":"evidencia-20260923155207","producer":"reserva-service","data":{"reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","clienteId":"33333333-3333-3333-3333-155207000000","pagamentoId":"ea2d6c35-e253-46bd-875b-c87f3cdb7fc2","valorTotal":79.80,"status":"PAGAMENTO_APROVADO"}}
Partition:0	Offset:2	eventId:914c5931-047b-4394-98c6-83c5671fef57,eventType:ReservaConfirmada,correlationId:evidencia-20260923155207,traceparent:00-6ab41fd89de166800237a007687112d3-5164b543f07e9f7e-01	893a19ad-9598-4083-9bb2-4029c44516b9	{"eventId":"914c5931-047b-4394-98c6-83c5671fef57","eventType":"ReservaConfirmada","eventVersion":1,"occurredAt":"2026-09-23T18:52:09.853121699Z","reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","correlationId":"evidencia-20260923155207","producer":"reserva-service","data":{"reservaId":"893a19ad-9598-4083-9bb2-4029c44516b9","clienteId":"33333333-3333-3333-3333-155207000000","sessaoId":"22222222-2222-2222-2222-222222222222","assentos":["A1","A2"],"valorTotal":79.80,"pagamentoId":"ea2d6c35-e253-46bd-875b-c87f3cdb7fc2","ingressoId":"69a714a0-cbe1-4c73-9e99-403840c5f17b","status":"CONFIRMADA"}}
```

![Kafka UI: tópico reserva.eventos com 6 partições](evidencias/kafka-ui-topico.png)

## 4. Consumo pelos serviços interessados

`notificacao-service` e `auditoria-service` processaram os três eventos, na ordem. O `fidelidade-service` recebeu só o `ReservaConfirmada` (o filtro por `eventType` descarta os outros tipos antes do listener).

```text
--- notificacao-service ---
2026-09-23 18:52:11.068 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=9c9e82fe44ee7817 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.recebido service=notificacao-service partition=0 offset=0 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:12.249 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=9c9e82fe44ee7817 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.processado.sucesso eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 destinatarioId=33333333-3333-3333-3333-155207000000 notificacaoId=2a35327b-617f-4702-9b64-ae0f185969f7 resultado=notificacao_registrada
2026-09-23 18:52:12.283 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=b1f13eb4ee54d4f6 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.recebido service=notificacao-service partition=0 offset=1 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:12.293 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=b1f13eb4ee54d4f6 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.processado.sucesso eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 destinatarioId=33333333-3333-3333-3333-155207000000 notificacaoId=d6c95a4b-8aec-4c8a-b982-7df0741cc5d9 resultado=notificacao_registrada
2026-09-23 18:52:12.304 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=70db620cab24a100 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.recebido service=notificacao-service partition=0 offset=2 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:12.313 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=70db620cab24a100 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.processado.sucesso eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 destinatarioId=33333333-3333-3333-3333-155207000000 notificacaoId=2057d790-a691-439b-aedc-7b85b45bfd23 resultado=notificacao_registrada
--- fidelidade-service ---
2026-09-23 18:52:11.110 INFO  service=fidelidade-service traceId=6ab41fd89de166800237a007687112d3 spanId=e11c1120fc06ac5d correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.f.i.m.ReservaEventListener - kafka.evento.recebido service=fidelidade-service partition=0 offset=2 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:11.986 INFO  service=fidelidade-service traceId=6ab41fd89de166800237a007687112d3 spanId=e11c1120fc06ac5d correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.f.a.FidelidadeApplicationService - fidelidade.evento.processado.sucesso eventId=914c5931-047b-4394-98c6-83c5671fef57 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 clienteId=33333333-3333-3333-3333-155207000000 reservasConfirmadas=1 pontos=79 resultado=historico_atualizado
--- auditoria-service ---
2026-09-23 18:52:11.205 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=c4e6dba7457eddb0 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.i.m.ReservaEventListener - kafka.evento.recebido service=auditoria-service partition=0 offset=0 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:12.023 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=c4e6dba7457eddb0 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.processado.sucesso auditoriaId=fd7c6775-23ba-4d0e-9906-c3fb81e479f3 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 particao=0 offset=0 resultado=auditoria_registrada
2026-09-23 18:52:12.120 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=b0c17f5f8548ba4c correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.i.m.ReservaEventListener - kafka.evento.recebido service=auditoria-service partition=0 offset=1 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:12.140 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=b0c17f5f8548ba4c correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.processado.sucesso auditoriaId=0015dfc3-c852-4a0e-a0a8-fda42d793d7a eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 particao=0 offset=1 resultado=auditoria_registrada
2026-09-23 18:52:12.178 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=be0677871c594d0b correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.i.m.ReservaEventListener - kafka.evento.recebido service=auditoria-service partition=0 offset=2 key=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 correlationId=evidencia-20260923155207
2026-09-23 18:52:12.203 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=be0677871c594d0b correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.processado.sucesso auditoriaId=d28bad3b-0711-43ec-8665-67eb6519cefd eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 particao=0 offset=2 resultado=auditoria_registrada

(fidelidade-service só recebe ReservaConfirmada: os demais tipos são descartados pelo filtro por eventType)
```

## 5. Persistência nos serviços consumidores

Cada consumidor gravou o resultado no seu próprio banco: três notificações, o histórico de fidelidade do cliente (1 reserva, 2 ingressos, R$ 79,80, 79 pontos) e a trilha de auditoria com partição e offset. A última linha é a mesma informação consultada pela API, via Gateway.

```text
--- notificacao_db ---
        tipo        |           destinatario_id            |                                mensagem                                 |           criada_em
--------------------+--------------------------------------+-------------------------------------------------------------------------+-------------------------------
 RESERVA_CRIADA     | 33333333-3333-3333-3333-155207000000 | Recebemos sua reserva dos assentos [A1, A2]. Aguardando o pagamento.    | 2026-09-23 18:52:12.187561+00
 PAGAMENTO_APROVADO | 33333333-3333-3333-3333-155207000000 | O pagamento de R$ 79.80 foi aprovado. Estamos emitindo seu ingresso.    | 2026-09-23 18:52:12.289747+00
 RESERVA_CONFIRMADA | 33333333-3333-3333-3333-155207000000 | Reserva confirmada! Seu ingresso para os assentos [A1, A2] foi emitido. | 2026-09-23 18:52:12.308392+00
(3 rows)

--- fidelidade_db ---
              cliente_id              | reservas_confirmadas | ingressos_comprados | valor_total_gasto | pontos
--------------------------------------+----------------------+---------------------+-------------------+--------
 33333333-3333-3333-3333-155207000000 |                    1 |                   2 |             79.80 |     79
(1 row)

--- auditoria_db ---
        event_type        |               event_id               |      correlation_id      |          occurred_at          |          recebido_em          | particao | offset_kafka
--------------------------+--------------------------------------+--------------------------+-------------------------------+-------------------------------+----------+--------------
 ReservaCriada            | 33bfceaa-1ff5-40f3-b700-45a28d1247f8 | evidencia-20260923155207 | 2026-09-23 18:52:08.355274+00 | 2026-09-23 18:52:11.964417+00 |        0 |            0
 ReservaPagamentoAprovado | 3ec38043-0349-4442-991c-b56de1ba9cc5 | evidencia-20260923155207 | 2026-09-23 18:52:09.200136+00 | 2026-09-23 18:52:12.136278+00 |        0 |            1
 ReservaConfirmada        | 914c5931-047b-4394-98c6-83c5671fef57 | evidencia-20260923155207 | 2026-09-23 18:52:09.853122+00 | 2026-09-23 18:52:12.194957+00 |        0 |            2
(3 rows)

--- consulta via API Gateway: GET /api/fidelidade/33333333-3333-3333-3333-155207000000 ---
{"clienteId":"33333333-3333-3333-3333-155207000000","reservasConfirmadas":1,"ingressosComprados":2,"valorTotalGasto":79.80,"pontos":79}
```

## 6. Mensagem duplicada sem efeitos colaterais

O mesmo `ReservaConfirmada` (mesmo `eventId`) foi republicado duas vezes no tópico. Pontos, notificações e registros de auditoria continuam iguais, e os três serviços registraram `evento.duplicado.ignorado`.

```text
eventId reenviado: 914c5931-047b-4394-98c6-83c5671fef57

ANTES da reentrega:
 reservas_confirmadas | pontos | valor_total_gasto
----------------------+--------+-------------------
                    1 |     79 |             79.80
(1 row)

 notificacoes_da_reserva
-------------------------
                       3
(1 row)

 registros_de_auditoria
------------------------
                      3
(1 row)

$ (republica a mesma mensagem 2x no tópico, com a mesma chave)

DEPOIS da reentrega (os valores devem ser os mesmos):
 reservas_confirmadas | pontos | valor_total_gasto
----------------------+--------+-------------------
                    1 |     79 |             79.80
(1 row)

 notificacoes_da_reserva
-------------------------
                       3
(1 row)

 registros_de_auditoria
------------------------
                      3
(1 row)

--- logs: reconhecimento da duplicidade ---
2026-09-23 18:52:47.665 INFO  service=notificacao-service traceId=6ab41fffd724928bde6b3c8de538d285 spanId=de6b3c8de538d285 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23 18:52:47.683 INFO  service=notificacao-service traceId=6ab41fffad26e19c0185342d4772f9c9 spanId=0185342d4772f9c9 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23 18:52:47.656 INFO  service=fidelidade-service traceId=6ab41fff09593646dfa18ed2d4d7e29c spanId=dfa18ed2d4d7e29c correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.f.a.FidelidadeApplicationService - fidelidade.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23 18:52:47.670 INFO  service=fidelidade-service traceId=6ab41fff081fdb6a9462888be19487b0 spanId=9462888be19487b0 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.f.a.FidelidadeApplicationService - fidelidade.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23 18:52:47.656 INFO  service=auditoria-service traceId=6ab41fff8cc3b865ed97e0ee07ad165d spanId=ed97e0ee07ad165d correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23 18:52:47.680 INFO  service=auditoria-service traceId=6ab41fffa9e98b9d685a4e5a3b4bbd65 spanId=685a4e5a3b4bbd65 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
```

## 7. Ordem por reserva e processamento concorrente

Seis reservas feitas em sequência rápida. Na auditoria, os eventos de cada reserva estão na mesma partição, com offsets e horários de processamento crescentes. As threads do listener mostram partições diferentes processadas por threads diferentes (3 threads por consumidor, 6 partições).

As reservas não são disparadas realmente em paralelo porque todas usam a mesma sessão: o Aggregate `Sessao` tem controle otimista de versão, e requisições simultâneas na mesma sessão são rejeitadas (proteção contra venda duplicada do assento). O que interessa aqui é o consumo concorrente.

```text
Criando 6 reservas em sequência rápida (assentos A3, A4, A5, B1, B2, B3)...
  A3: HTTP/1.1 201 Created
  A4: HTTP/1.1 201 Created
  A5: HTTP/1.1 201 Created
  B1: HTTP/1.1 201 Created
  B2: HTTP/1.1 201 Created
  B3: HTTP/1.1 201 Created
--- auditoria: por reserva, a ordem de processamento (recebido_em) segue a ordem de produção (offset) ---
              reserva_id              |        event_type        | particao | offset_kafka |          recebido_em
--------------------------------------+--------------------------+----------+--------------+-------------------------------
 21925f3e-1156-4a78-a0ac-abda21b43bf8 | ReservaCriada            |        0 |            5 | 2026-09-23 18:52:59.191402+00
 21925f3e-1156-4a78-a0ac-abda21b43bf8 | ReservaPagamentoAprovado |        0 |            6 | 2026-09-23 18:52:59.222759+00
 21925f3e-1156-4a78-a0ac-abda21b43bf8 | ReservaConfirmada        |        0 |            7 | 2026-09-23 18:52:59.265388+00
 3ac54f22-c883-46a6-9905-49faa7566c57 | ReservaCriada            |        5 |            6 | 2026-09-23 18:52:59.363626+00
 3ac54f22-c883-46a6-9905-49faa7566c57 | ReservaPagamentoAprovado |        5 |            7 | 2026-09-23 18:52:59.421657+00
 3ac54f22-c883-46a6-9905-49faa7566c57 | ReservaConfirmada        |        5 |            8 | 2026-09-23 18:52:59.602257+00
 5c3c5b52-b0d1-41a7-a5ac-40795a9663a9 | ReservaCriada            |        4 |            0 | 2026-09-23 18:52:59.297456+00
 5c3c5b52-b0d1-41a7-a5ac-40795a9663a9 | ReservaPagamentoAprovado |        4 |            1 | 2026-09-23 18:52:59.331348+00
 5c3c5b52-b0d1-41a7-a5ac-40795a9663a9 | ReservaConfirmada        |        4 |            2 | 2026-09-23 18:52:59.398616+00
 6e26c888-033b-4e12-949c-bba81166faa7 | ReservaCriada            |        5 |            3 | 2026-09-23 18:52:58.611696+00
 6e26c888-033b-4e12-949c-bba81166faa7 | ReservaPagamentoAprovado |        5 |            4 | 2026-09-23 18:52:58.652549+00
 6e26c888-033b-4e12-949c-bba81166faa7 | ReservaConfirmada        |        5 |            5 | 2026-09-23 18:52:58.670636+00
 73ad50de-d074-4b8d-8ec6-f55f9ab7e143 | ReservaCriada            |        3 |            0 | 2026-09-23 18:52:58.389144+00
 73ad50de-d074-4b8d-8ec6-f55f9ab7e143 | ReservaPagamentoAprovado |        3 |            1 | 2026-09-23 18:52:58.43073+00
 73ad50de-d074-4b8d-8ec6-f55f9ab7e143 | ReservaConfirmada        |        3 |            2 | 2026-09-23 18:52:58.523856+00
 dcecb85f-d4fb-4b88-95d2-e01e675100b2 | ReservaCriada            |        5 |            0 | 2026-09-23 18:52:58.510569+00
 dcecb85f-d4fb-4b88-95d2-e01e675100b2 | ReservaPagamentoAprovado |        5 |            1 | 2026-09-23 18:52:58.547564+00
 dcecb85f-d4fb-4b88-95d2-e01e675100b2 | ReservaConfirmada        |        5 |            2 | 2026-09-23 18:52:58.582069+00
(18 rows)

--- threads do listener (auditoria-service): partições diferentes processadas por threads diferentes ---
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 partition=0 ReservaConfirmada reserva=21925f3e-1156-4a78-a0ac-abda21b43bf8
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 partition=0 ReservaCriada reserva=21925f3e-1156-4a78-a0ac-abda21b43bf8
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 partition=0 ReservaPagamentoAprovado reserva=21925f3e-1156-4a78-a0ac-abda21b43bf8
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-1-C-1 partition=3 ReservaConfirmada reserva=73ad50de-d074-4b8d-8ec6-f55f9ab7e143
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-1-C-1 partition=3 ReservaCriada reserva=73ad50de-d074-4b8d-8ec6-f55f9ab7e143
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-1-C-1 partition=3 ReservaPagamentoAprovado reserva=73ad50de-d074-4b8d-8ec6-f55f9ab7e143
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=4 ReservaConfirmada reserva=5c3c5b52-b0d1-41a7-a5ac-40795a9663a9
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=4 ReservaCriada reserva=5c3c5b52-b0d1-41a7-a5ac-40795a9663a9
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=4 ReservaPagamentoAprovado reserva=5c3c5b52-b0d1-41a7-a5ac-40795a9663a9
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaConfirmada reserva=3ac54f22-c883-46a6-9905-49faa7566c57
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaConfirmada reserva=6e26c888-033b-4e12-949c-bba81166faa7
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaConfirmada reserva=dcecb85f-d4fb-4b88-95d2-e01e675100b2
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaCriada reserva=3ac54f22-c883-46a6-9905-49faa7566c57
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaCriada reserva=6e26c888-033b-4e12-949c-bba81166faa7
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaCriada reserva=dcecb85f-d4fb-4b88-95d2-e01e675100b2
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaPagamentoAprovado reserva=3ac54f22-c883-46a6-9905-49faa7566c57
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaPagamentoAprovado reserva=6e26c888-033b-4e12-949c-bba81166faa7
thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 partition=5 ReservaPagamentoAprovado reserva=dcecb85f-d4fb-4b88-95d2-e01e675100b2
```

## 8. Falha: retentativas e dead-letter topic

Uma mensagem que não é JSON válido foi publicada no tópico. O `notificacao-service` fez 4 tentativas (1 + 3 retentativas, 1 s de intervalo) e encaminhou a mensagem para `reserva.eventos.notificacao.dlt`, com a causa e a origem nos headers `kafka_dlt-*`.

```text
$ publica em reserva.eventos uma mensagem que não é JSON válido (chave=mensagem-invalida-1790189587)
--- notificacao-service: falha registrada, retentativas e envio ao DLT ---
2026-09-23 18:53:10.033 ERROR service=notificacao-service traceId=6ab42016ab8d568c0d6e713bb1f89078 spanId=0d6e713bb1f89078 correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.falha service=notificacao-service etapa=desserializacao partition=4 offset=3 key=mensagem-invalida-1790189587 motivo=Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:10.049 WARN  service=notificacao-service traceId=6ab42016ab8d568c0d6e713bb1f89078 spanId=0d6e713bb1f89078 correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.KafkaConsumerConfig - kafka.evento.retry topic=reserva.eventos key=mensagem-invalida-1790189587 tentativa=1 motivo=StreamReadException: Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:11.078 ERROR service=notificacao-service traceId=6ab420179553610ee1e4bacd2e1fdb5b spanId=e1e4bacd2e1fdb5b correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.falha service=notificacao-service etapa=desserializacao partition=4 offset=3 key=mensagem-invalida-1790189587 motivo=Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:11.079 WARN  service=notificacao-service traceId=6ab420179553610ee1e4bacd2e1fdb5b spanId=e1e4bacd2e1fdb5b correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.KafkaConsumerConfig - kafka.evento.retry topic=reserva.eventos key=mensagem-invalida-1790189587 tentativa=2 motivo=StreamReadException: Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:12.086 ERROR service=notificacao-service traceId=6ab42018b1b3f2fb90019a659a756dac spanId=90019a659a756dac correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.falha service=notificacao-service etapa=desserializacao partition=4 offset=3 key=mensagem-invalida-1790189587 motivo=Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:12.087 WARN  service=notificacao-service traceId=6ab42018b1b3f2fb90019a659a756dac spanId=90019a659a756dac correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.KafkaConsumerConfig - kafka.evento.retry topic=reserva.eventos key=mensagem-invalida-1790189587 tentativa=3 motivo=StreamReadException: Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:13.100 ERROR service=notificacao-service traceId=6ab42019a58769ef83fc6d0e73dd011b spanId=83fc6d0e73dd011b correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.ReservaEventListener - kafka.evento.falha service=notificacao-service etapa=desserializacao partition=4 offset=3 key=mensagem-invalida-1790189587 motivo=Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:13.101 WARN  service=notificacao-service traceId=6ab42019a58769ef83fc6d0e73dd011b spanId=83fc6d0e73dd011b correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.KafkaConsumerConfig - kafka.evento.retry topic=reserva.eventos key=mensagem-invalida-1790189587 tentativa=4 motivo=StreamReadException: Unexpected character ('m' (code 109)): was expecting double-quote to start property name
2026-09-23 18:53:13.102 ERROR service=notificacao-service traceId=6ab42019a58769ef83fc6d0e73dd011b spanId=83fc6d0e73dd011b correlationId= reservaId= eventId= thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-2-C-1 logger=b.c.c.n.i.m.KafkaConsumerConfig - kafka.evento.dlt.encaminhado topicOrigem=reserva.eventos partition=4 offset=3 key=mensagem-invalida-1790189587 destino=reserva.eventos.notificacao.dlt motivo=StreamReadException: Unexpected character ('m' (code 109)): was expecting double-quote to start property name

--- mensagem no DLT, com a causa da falha e a origem nos headers ---
Partition:1	Offset:0
kafka_dlt-exception-cause-fqcn:tools.jackson.core.exc.StreamReadException
kafka_dlt-original-topic:reserva.eventos
kafka_dlt-original-consumer-group:notificacao-service
chave e valor: mensagem-invalida-1790189587	{ mensagem corrompida
```

![Kafka UI: mensagens no DLT do notificacao-service](evidencias/kafka-ui-dlt.png)

## 9. Logs centralizados (Elasticsearch / Kibana)

Uma única consulta pelo `correlationId` traz as linhas de log de **todos** os serviços que participaram da operação, em ordem cronológica, sem abrir o console de nenhum deles. A mesma busca pode ser feita pelo `reservaId`.

```text
--- linhas de log por serviço para este correlationId ---
      service      |    linhas
-------------------+---------------
api-gateway        |2
auditoria-service  |10
fidelidade-service |6
ingresso-service   |2
notificacao-service|10
pagamento-service  |2
reserva-service    |11

--- linhas de log de todos os serviços, em ordem cronológica ---
       @timestamp       |      service      |                                                   message
------------------------+-------------------+--------------------------------------------------------------------------------------------------------------
2026-09-23T18:52:08.233Z|api-gateway        |gateway.request.inicio method=POST path=/api/reservas correlationId=evidencia-20260923155207
2026-09-23T18:52:08.344Z|reserva-service    |reserva.realizacao.inicio clienteId=33333333-3333-3333-3333-155207000000 sessaoId=22222222-2222-2222-2222-2222
2026-09-23T18:52:08.388Z|reserva-service    |reserva.persistida reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=AGUARDANDO_PAGAMENTO eventos=1
2026-09-23T18:52:08.388Z|reserva-service    |reserva.outbox.registrado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28
2026-09-23T18:52:08.953Z|pagamento-service  |pagamento.processamento.inicio reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 valor=79.80
2026-09-23T18:52:09.046Z|pagamento-service  |pagamento.processamento.fim reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 pagamentoId=ea2d6c35-e253-46bd-875b
2026-09-23T18:52:09.205Z|reserva-service    |reserva.outbox.registrado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de
2026-09-23T18:52:09.205Z|reserva-service    |reserva.persistida reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=EMITINDO_INGRESSO eventos=1
2026-09-23T18:52:09.610Z|ingresso-service   |ingresso.emissao.inicio reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 assentos=[A1, A2]
2026-09-23T18:52:09.728Z|ingresso-service   |ingresso.emissao.fim reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 ingressoId=69a714a0-cbe1-4c73-9e99-403840c
2026-09-23T18:52:09.862Z|reserva-service    |reserva.outbox.registrado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c56
2026-09-23T18:52:09.863Z|reserva-service    |reserva.persistida reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=CONFIRMADA eventos=1
2026-09-23T18:52:09.865Z|reserva-service    |reserva.realizacao.sucesso reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=CONFIRMADA pagamentoId=ea2d6c
2026-09-23T18:52:09.923Z|api-gateway        |gateway.request.fim method=POST path=/api/reservas status=201 CREATED durationMs=1690 correlationId=evidencia-
2026-09-23T18:52:10.478Z|reserva-service    |reserva.outbox.publicado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d
2026-09-23T18:52:10.507Z|reserva-service    |reserva.outbox.publicado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1
2026-09-23T18:52:10.527Z|reserva-service    |reserva.outbox.publicado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c567
2026-09-23T18:52:11.068Z|notificacao-service|kafka.evento.recebido service=notificacao-service partition=0 offset=0 key=893a19ad-9598-4083-9bb2-4029c44516b
2026-09-23T18:52:11.110Z|fidelidade-service |kafka.evento.recebido service=fidelidade-service partition=0 offset=2 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:11.205Z|auditoria-service  |kafka.evento.recebido service=auditoria-service partition=0 offset=0 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:11.986Z|fidelidade-service |fidelidade.evento.processado.sucesso eventId=914c5931-047b-4394-98c6-83c5671fef57 reservaId=893a19ad-9598-4083
2026-09-23T18:52:12.023Z|auditoria-service  |auditoria.evento.processado.sucesso auditoriaId=fd7c6775-23ba-4d0e-9906-c3fb81e479f3 eventId=33bfceaa-1ff5-40f
2026-09-23T18:52:12.120Z|auditoria-service  |kafka.evento.recebido service=auditoria-service partition=0 offset=1 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:12.140Z|auditoria-service  |auditoria.evento.processado.sucesso auditoriaId=0015dfc3-c852-4a0e-a0a8-fda42d793d7a eventId=3ec38043-0349-444
2026-09-23T18:52:12.178Z|auditoria-service  |kafka.evento.recebido service=auditoria-service partition=0 offset=2 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:12.203Z|auditoria-service  |auditoria.evento.processado.sucesso auditoriaId=d28bad3b-0711-43ec-8665-67eb6519cefd eventId=914c5931-047b-439
2026-09-23T18:52:12.249Z|notificacao-service|notificacao.evento.processado.sucesso eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada res
2026-09-23T18:52:12.283Z|notificacao-service|kafka.evento.recebido service=notificacao-service partition=0 offset=1 key=893a19ad-9598-4083-9bb2-4029c44516b
2026-09-23T18:52:12.293Z|notificacao-service|notificacao.evento.processado.sucesso eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoA
2026-09-23T18:52:12.304Z|notificacao-service|kafka.evento.recebido service=notificacao-service partition=0 offset=2 key=893a19ad-9598-4083-9bb2-4029c44516b
2026-09-23T18:52:12.313Z|notificacao-service|notificacao.evento.processado.sucesso eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada
2026-09-23T18:52:47.649Z|fidelidade-service |kafka.evento.recebido service=fidelidade-service partition=0 offset=3 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:47.649Z|auditoria-service  |kafka.evento.recebido service=auditoria-service partition=0 offset=3 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:47.653Z|notificacao-service|kafka.evento.recebido service=notificacao-service partition=0 offset=3 key=893a19ad-9598-4083-9bb2-4029c44516b
2026-09-23T18:52:47.656Z|fidelidade-service |fidelidade.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada
2026-09-23T18:52:47.656Z|auditoria-service  |auditoria.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada r
2026-09-23T18:52:47.664Z|fidelidade-service |kafka.evento.recebido service=fidelidade-service partition=0 offset=4 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:47.665Z|notificacao-service|notificacao.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada
2026-09-23T18:52:47.670Z|fidelidade-service |fidelidade.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada
2026-09-23T18:52:47.676Z|auditoria-service  |kafka.evento.recebido service=auditoria-service partition=0 offset=4 key=893a19ad-9598-4083-9bb2-4029c44516b9
2026-09-23T18:52:47.679Z|notificacao-service|kafka.evento.recebido service=notificacao-service partition=0 offset=4 key=893a19ad-9598-4083-9bb2-4029c44516b
2026-09-23T18:52:47.680Z|auditoria-service  |auditoria.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada r
2026-09-23T18:52:47.683Z|notificacao-service|notificacao.evento.duplicado.ignorado eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada


==================================================================
   busca por reservaId=893a19ad-9598-4083-9bb2-4029c44516b9
==================================================================
      service      |    linhas
-------------------+---------------
auditoria-service  |10
fidelidade-service |6
notificacao-service|10
reserva-service    |15
```

![Kibana: busca por correlationId.keyword](evidencias/kibana-correlationid.png)

## 10. Propagação do correlationId

O mesmo `correlationId` no Gateway, no reserva-service, nas chamadas HTTP internas para o pagamento e o ingresso, na outbox, no Kafka (header e payload) e nos três consumidores.

```text
--- API Gateway ---
2026-09-23 18:52:08.233 INFO  service=api-gateway traceId=6ab41fd89de166800237a007687112d3 spanId=0237a007687112d3 correlationId= thread=reactor-http-epoll-2 logger=b.c.c.g.CorrelationIdGatewayFilter - gateway.request.inicio method=POST path=/api/reservas correlationId=evidencia-20260923155207
--- reserva-service (HTTP) ---
2026-09-23 18:52:09.865 INFO  service=reserva-service traceId=6ab41fd89de166800237a007687112d3 spanId=2720be7d0b0420c2 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 thread=http-nio-8081-exec-2 logger=b.c.c.r.a.ReservaApplicationService - reserva.realizacao.sucesso reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 status=CONFIRMADA pagamentoId=ea2d6c35-e253-46bd-875b-c87f3cdb7fc2 ingressoId=69a714a0-cbe1-4c73-9e99-403840c5f17b
--- pagamento-service (HTTP interno) ---
2026-09-23 18:52:08.953 INFO  service=pagamento-service traceId=6ab41fd89de166800237a007687112d3 spanId=297b9126fb571156 correlationId=evidencia-20260923155207 thread=http-nio-8082-exec-2 logger=b.c.c.p.a.PagamentoApplicationService - pagamento.processamento.inicio reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 valor=79.80
2026-09-23 18:52:09.046 INFO  service=pagamento-service traceId=6ab41fd89de166800237a007687112d3 spanId=297b9126fb571156 correlationId=evidencia-20260923155207 thread=http-nio-8082-exec-2 logger=b.c.c.p.a.PagamentoApplicationService - pagamento.processamento.fim reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 pagamentoId=ea2d6c35-e253-46bd-875b-c87f3cdb7fc2 status=APROVADO
--- ingresso-service (HTTP interno) ---
2026-09-23 18:52:09.610 INFO  service=ingresso-service traceId=6ab41fd89de166800237a007687112d3 spanId=eb8b7b68c97f3000 correlationId=evidencia-20260923155207 thread=http-nio-8083-exec-1 logger=b.c.c.i.a.IngressoApplicationService - ingresso.emissao.inicio reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 assentos=[A1, A2]
2026-09-23 18:52:09.728 INFO  service=ingresso-service traceId=6ab41fd89de166800237a007687112d3 spanId=eb8b7b68c97f3000 correlationId=evidencia-20260923155207 thread=http-nio-8083-exec-1 logger=b.c.c.i.a.IngressoApplicationService - ingresso.emissao.fim reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 ingressoId=69a714a0-cbe1-4c73-9e99-403840c5f17b codigo=CINE-84E7AF30
--- reserva-service (outbox) ---
        event_type        |      correlation_id
--------------------------+--------------------------
 ReservaCriada            | evidencia-20260923155207
 ReservaPagamentoAprovado | evidencia-20260923155207
 ReservaConfirmada        | evidencia-20260923155207
(3 rows)

--- Kafka (header + payload) ---
      3 "correlationId":"evidencia-20260923155207"
      3 correlationId:evidencia-20260923155207
--- notificacao-service ---
2026-09-23 18:52:12.249 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=9c9e82fe44ee7817 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.processado.sucesso eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 destinatarioId=33333333-3333-3333-3333-155207000000 notificacaoId=2a35327b-617f-4702-9b64-ae0f185969f7 resultado=notificacao_registrada
2026-09-23 18:52:12.293 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=b1f13eb4ee54d4f6 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.processado.sucesso eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 destinatarioId=33333333-3333-3333-3333-155207000000 notificacaoId=d6c95a4b-8aec-4c8a-b982-7df0741cc5d9 resultado=notificacao_registrada
2026-09-23 18:52:12.313 INFO  service=notificacao-service traceId=6ab41fd89de166800237a007687112d3 spanId=70db620cab24a100 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.n.a.NotificacaoApplicationService - notificacao.evento.processado.sucesso eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 destinatarioId=33333333-3333-3333-3333-155207000000 notificacaoId=2057d790-a691-439b-aedc-7b85b45bfd23 resultado=notificacao_registrada
--- fidelidade-service ---
2026-09-23 18:52:11.986 INFO  service=fidelidade-service traceId=6ab41fd89de166800237a007687112d3 spanId=e11c1120fc06ac5d correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.f.a.FidelidadeApplicationService - fidelidade.evento.processado.sucesso eventId=914c5931-047b-4394-98c6-83c5671fef57 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 clienteId=33333333-3333-3333-3333-155207000000 reservasConfirmadas=1 pontos=79 resultado=historico_atualizado
--- auditoria-service ---
2026-09-23 18:52:12.023 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=c4e6dba7457eddb0 correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.processado.sucesso auditoriaId=fd7c6775-23ba-4d0e-9906-c3fb81e479f3 eventId=33bfceaa-1ff5-40f3-b700-45a28d1247f8 eventType=ReservaCriada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 particao=0 offset=0 resultado=auditoria_registrada
2026-09-23 18:52:12.140 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=b0c17f5f8548ba4c correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.processado.sucesso auditoriaId=0015dfc3-c852-4a0e-a0a8-fda42d793d7a eventId=3ec38043-0349-4442-991c-b56de1ba9cc5 eventType=ReservaPagamentoAprovado reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 particao=0 offset=1 resultado=auditoria_registrada
2026-09-23 18:52:12.203 INFO  service=auditoria-service traceId=6ab41fd89de166800237a007687112d3 spanId=be0677871c594d0b correlationId=evidencia-20260923155207 reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 eventId=914c5931-047b-4394-98c6-83c5671fef57 thread=org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1 logger=b.c.c.a.a.AuditoriaApplicationService - auditoria.evento.processado.sucesso auditoriaId=d28bad3b-0711-43ec-8665-67eb6519cefd eventId=914c5931-047b-4394-98c6-83c5671fef57 eventType=ReservaConfirmada reservaId=893a19ad-9598-4083-9bb2-4029c44516b9 particao=0 offset=2 resultado=auditoria_registrada
```

## 11. Trace no Zipkin

Um único trace cobre a operação inteira: Gateway → reserva-service → pagamento/ingresso (HTTP) → publicação da outbox → Kafka → três consumidores.

```text
traceId da reserva: 6ab41fd89de166800237a007687112d3
--- serviços e spans do trace (API do Zipkin: /api/v2/trace/6ab41fd89de166800237a007687112d3) ---
      2 api-gateway  ->  http post
      3 auditoria-service  ->  reserva.eventos process
      3 fidelidade-service  ->  reserva.eventos process
      1 ingresso-service  ->  http post /api/ingressos
      3 notificacao-service  ->  reserva.eventos process
      1 pagamento-service  ->  http post /api/pagamentos
      2 reserva-service  ->  http post
      1 reserva-service  ->  http post /api/reservas
      1 reserva-service  ->  outbox publicar reservaconfirmada
      1 reserva-service  ->  outbox publicar reservacriada
      1 reserva-service  ->  outbox publicar reservapagamentoaprovado
      3 reserva-service  ->  reserva.eventos send

Visualização: http://localhost:9411/zipkin/traces/6ab41fd89de166800237a007687112d3
```

![Zipkin: trace único da reserva](evidencias/zipkin-trace.png)

## 12. Reprocessamento após falha transitória

Com o PostgreSQL parado, um `ReservaConfirmada` falhou 4 vezes no `fidelidade-service` e foi para o DLT. Depois de o banco voltar, `./infra/scripts/reprocessar-dlt.sh fidelidade` republicou o evento no tópico original (mesma chave) e ele foi aplicado uma única vez. A mensagem malformada do cenário 8, que também tinha ido para o DLT da fidelidade, não é republicada: o script só reprocessa eventos válidos e deixa as mensagens malformadas no DLT para análise.

```text
$ docker stop cinepass-postgres   (simula indisponibilidade do banco dos consumidores)
$ publica um ReservaConfirmada (eventId=66666666-6666-6666-6666-155357000000, reservaId=55555555-5555-5555-5555-155357000000, clienteId=44444444-4444-4444-4444-155357000000) no tópico

--- retentativas e envio ao DLT (fidelidade-service) ---
kafka.evento.retry topic=reserva.eventos key=55555555-5555-5555-5555-155357000000 tentativa=1 motivo=PSQLException: This connection has been closed.
kafka.evento.retry topic=reserva.eventos key=55555555-5555-5555-5555-155357000000 tentativa=2 motivo=UnknownHostException: postgres
kafka.evento.retry topic=reserva.eventos key=55555555-5555-5555-5555-155357000000 tentativa=3 motivo=UnknownHostException: postgres
kafka.evento.retry topic=reserva.eventos key=55555555-5555-5555-5555-155357000000 tentativa=4 motivo=UnknownHostException: postgres
kafka.evento.dlt.encaminhado topicOrigem=reserva.eventos partition=3 offset=3 key=55555555-5555-5555-5555-155357000000 destino=reserva.eventos.fidelidade.dlt motivo=UnknownHostException: postgres

$ docker start cinepass-postgres   (causa corrigida)

ANTES do reprocessamento:
 historico_do_cliente
----------------------
                    0
(1 row)

$ ./infra/scripts/reprocessar-dlt.sh fidelidade
Lendo mensagens pendentes de reserva.eventos.fidelidade.dlt...
Ignorando 1 mensagem(ns) que não são eventos válidos (ficam no DLT para análise):
  chave=mensagem-invalida-1790189587
Republicando 1 mensagem(ns) em reserva.eventos...
Concluído.

DEPOIS do reprocessamento:
              cliente_id              | reservas_confirmadas | ingressos_comprados | valor_total_gasto | pontos
--------------------------------------+----------------------+---------------------+-------------------+--------
 44444444-4444-4444-4444-155357000000 |                    1 |                   2 |             79.80 |     79
(1 row)

fidelidade.evento.processado.sucesso eventId=66666666-6666-6666-6666-155357000000 reservaId=55555555-5555-5555-5555-155357000000 clienteId=44444444-4444-4444-4444-155357000000 reservasConfirmadas=1 pontos=79 resultado=historico_atualizado
```

## 13. Testes automatizados

`mvn verify` na raiz: testes de unidade e testes de integração com Testcontainers (PostgreSQL e Kafka reais). O mesmo comando roda no GitHub Actions (`.github/workflows/ci.yml`).

```text
Reactor Summary for CinePass Saga Base 1.0.0-SNAPSHOT:

discovery-server ................................... SUCCESS [  4.250 s]
api-gateway ........................................ SUCCESS [  1.273 s]
reserva-service .................................... SUCCESS [01:43 min]
pagamento-service .................................. SUCCESS [  5.078 s]
ingresso-service ................................... SUCCESS [  5.560 s]
notificacao-service ................................ SUCCESS [01:24 min]
fidelidade-service ................................. SUCCESS [01:41 min]
auditoria-service .................................. SUCCESS [01:24 min]
CinePass Saga Base ................................. SUCCESS [  0.001 s]
------------------------------------------------------------------------
BUILD SUCCESS

Tests run: 1, Failures: 0, Errors: 0, (0.438 s) reserva.domain.model.ReservaTest
Tests run: 1, Failures: 0, Errors: 0, (0.064 s) reserva.domain.model.SessaoTest
Tests run: 2, Failures: 0, Errors: 0, (73.10 s) reserva.OutboxReservaIntegrationTest
Tests run: 1, Failures: 0, Errors: 0, (0.425 s) pagamento.domain.model.PagamentoTest
Tests run: 1, Failures: 0, Errors: 0, (0.277 s) ingresso.domain.model.IngressoTest
Tests run: 2, Failures: 0, Errors: 0, (70.06 s) notificacao.NotificacaoConsumidorIntegrationTest
Tests run: 1, Failures: 0, Errors: 0, (1.281 s) fidelidade.domain.model.ClienteFidelidadeTest
Tests run: 1, Failures: 0, Errors: 0, (74.91 s) fidelidade.IdempotenciaFidelidadeIntegrationTest
Tests run: 1, Failures: 0, Errors: 0, (70.09 s) auditoria.OrdenacaoPorReservaIntegrationTest

BUILD SUCCESS
```

# Contrato de comunicação entre os microsserviços

Especificação dos eventos trocados via Apache Kafka entre o `reserva-service`
(produtor) e os serviços `notificacao-service`, `fidelidade-service` e
`auditoria-service` (consumidores). Qualquer mudança de payload deve ser
refletida aqui.

## Visão geral

```text
reserva-service (produtor, Aggregate Reserva)
        │  Transactional Outbox
        ▼
   reserva.eventos  (6 partições, chave = reservaId)
        │
        ├──────────────────┬───────────────────────┬─────────────────────┐
        ▼                  ▼                       ▼
notificacao-service   fidelidade-service      auditoria-service
(todos os tipos)      (só ReservaConfirmada)  (todos os tipos)
```

É usado **um único tópico** para todo o ciclo de vida da reserva, e não um
tópico por tipo de evento. O Kafka só garante ordem dentro de uma partição: se
`ReservaCriada` e `ReservaConfirmada` fossem para tópicos diferentes, um
consumidor poderia ler a confirmação antes da criação. Com um tópico e a chave
`reservaId`, todos os eventos de uma reserva caem na mesma partição, em ordem.

## Tópicos

| Tópico | Partições | Produtor | Descrição |
|---|---|---|---|
| `reserva.eventos` | 6 | reserva-service | Eventos do ciclo de vida da reserva |
| `reserva.eventos.notificacao.dlt` | 3 | notificacao-service | Mensagens que falharam após as tentativas |
| `reserva.eventos.fidelidade.dlt` | 3 | fidelidade-service | Idem |
| `reserva.eventos.auditoria.dlt` | 3 | auditoria-service | Idem |

Os tópicos são criados pelo serviço `kafka-init` do `docker-compose.yml` (e
pelo `NewTopic` do reserva-service). A criação automática no broker está
desligada, para que nenhum tópico nasça com 1 partição por acidente.

## Chave de publicação

A chave de toda mensagem é o **`reservaId`**. O particionador do Kafka calcula
um hash da chave, então a mesma reserva vai sempre para a mesma partição.
Reservas diferentes se espalham pelas 6 partições e podem ser processadas em
paralelo.

## Envelope comum

```json
{
  "eventId": "2c1f9a0e-8b5d-4b8e-9d1a-4f7c2e6b3a10",
  "eventType": "ReservaConfirmada",
  "eventVersion": 1,
  "occurredAt": "2026-09-23T14:05:47.010Z",
  "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
  "correlationId": "teste-reserva-001",
  "producer": "reserva-service",
  "data": { }
}
```

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `eventId` | UUID | sim | Identificador único do evento; chave de idempotência dos consumidores. |
| `eventType` | string | sim | `ReservaCriada`, `ReservaPagamentoAprovado`, `ReservaConfirmada` ou `ReservaCancelada`. |
| `eventVersion` | int | sim | Versão do formato do payload. |
| `occurredAt` | ISO-8601 (UTC) | sim | Quando o evento de domínio ocorreu. |
| `reservaId` | UUID | sim | Reserva relacionada (Aggregate de origem). |
| `correlationId` | string | sim | Correlação da operação, propagada desde o API Gateway (ou gerada pelo reserva-service quando a chamada chega sem o header). No fluxo Temporal, chega às activities pelo `CorrelationIdContextPropagator`. |
| `producer` | string | sim | `reserva-service`. |
| `data` | objeto | sim | Dados específicos de cada evento. |

Cada mensagem também leva os headers Kafka `eventId`, `eventType` e
`correlationId`, além do `traceparent` (contexto de tracing W3C) adicionado pelo
Micrometer.

---

## Evento `ReservaCriada`

| | |
|---|---|
| Tópico | `reserva.eventos` |
| Produtor | reserva-service |
| Consumidores | notificacao-service, auditoria-service |
| Chave | `reservaId` |
| Quando | A reserva é criada e os assentos são bloqueados (`POST /api/reservas`). |

| Campo de `data` | Tipo | Obrigatório |
|---|---|---|
| `reservaId` | UUID | sim |
| `clienteId` | UUID | sim |
| `sessaoId` | UUID | sim |
| `assentos` | lista de string | sim |
| `valorTotal` | decimal | sim |
| `status` | string (`AGUARDANDO_PAGAMENTO`) | sim |

```json
{
  "eventId": "a1b2c3d4-0000-0000-0000-000000000001",
  "eventType": "ReservaCriada",
  "eventVersion": 1,
  "occurredAt": "2026-09-23T14:05:46.100Z",
  "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
  "correlationId": "teste-reserva-001",
  "producer": "reserva-service",
  "data": {
    "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
    "clienteId": "33333333-3333-3333-3333-333333333333",
    "sessaoId": "22222222-2222-2222-2222-222222222222",
    "assentos": ["A1", "A2"],
    "valorTotal": 79.80,
    "status": "AGUARDANDO_PAGAMENTO"
  }
}
```

## Evento `ReservaPagamentoAprovado`

| | |
|---|---|
| Tópico | `reserva.eventos` |
| Produtor | reserva-service |
| Consumidores | notificacao-service, auditoria-service |
| Chave | `reservaId` |
| Quando | O pagamento foi aprovado (`AGUARDANDO_PAGAMENTO → PAGAMENTO_APROVADO`). |

| Campo de `data` | Tipo | Obrigatório |
|---|---|---|
| `reservaId` | UUID | sim |
| `clienteId` | UUID | sim |
| `pagamentoId` | UUID | sim |
| `valorTotal` | decimal | sim |
| `status` | string (`PAGAMENTO_APROVADO`) | sim |

```json
{
  "eventId": "a1b2c3d4-0000-0000-0000-000000000002",
  "eventType": "ReservaPagamentoAprovado",
  "eventVersion": 1,
  "occurredAt": "2026-09-23T14:05:46.600Z",
  "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
  "correlationId": "teste-reserva-001",
  "producer": "reserva-service",
  "data": {
    "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
    "clienteId": "33333333-3333-3333-3333-333333333333",
    "pagamentoId": "9e8d7c6b-5a49-4382-a1b0-c9d8e7f6a5b4",
    "valorTotal": 79.80,
    "status": "PAGAMENTO_APROVADO"
  }
}
```

## Evento `ReservaConfirmada`

| | |
|---|---|
| Tópico | `reserva.eventos` |
| Produtor | reserva-service |
| Consumidores | notificacao-service, **fidelidade-service**, auditoria-service |
| Chave | `reservaId` |
| Quando | O ingresso foi emitido e a reserva confirmada (`EMITINDO_INGRESSO → CONFIRMADA`). |

| Campo de `data` | Tipo | Obrigatório |
|---|---|---|
| `reservaId` | UUID | sim |
| `clienteId` | UUID | sim |
| `sessaoId` | UUID | sim |
| `assentos` | lista de string | sim |
| `valorTotal` | decimal | sim |
| `pagamentoId` | UUID | sim |
| `ingressoId` | UUID | sim |
| `status` | string (`CONFIRMADA`) | sim |

```json
{
  "eventId": "a1b2c3d4-0000-0000-0000-000000000003",
  "eventType": "ReservaConfirmada",
  "eventVersion": 1,
  "occurredAt": "2026-09-23T14:05:47.010Z",
  "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
  "correlationId": "teste-reserva-001",
  "producer": "reserva-service",
  "data": {
    "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
    "clienteId": "33333333-3333-3333-3333-333333333333",
    "sessaoId": "22222222-2222-2222-2222-222222222222",
    "assentos": ["A1", "A2"],
    "valorTotal": 79.80,
    "pagamentoId": "9e8d7c6b-5a49-4382-a1b0-c9d8e7f6a5b4",
    "ingressoId": "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
    "status": "CONFIRMADA"
  }
}
```

É o único evento que altera o histórico do cliente no `fidelidade-service`
(soma reserva, ingressos, valor e pontos). É também o mais sensível à
duplicidade, pois a operação é aditiva.

## Evento `ReservaCancelada`

| | |
|---|---|
| Tópico | `reserva.eventos` |
| Produtor | reserva-service |
| Consumidores | notificacao-service, auditoria-service |
| Chave | `reservaId` |
| Quando | A reserva é cancelada e os assentos liberados (compensação da Saga no fluxo Temporal). |

| Campo de `data` | Tipo | Obrigatório |
|---|---|---|
| `reservaId` | UUID | sim |
| `clienteId` | UUID | sim |
| `sessaoId` | UUID | sim |
| `assentos` | lista de string | sim |
| `statusAnterior` | string | sim |
| `status` | string (`CANCELADA`) | sim |

```json
{
  "eventId": "a1b2c3d4-0000-0000-0000-000000000004",
  "eventType": "ReservaCancelada",
  "eventVersion": 1,
  "occurredAt": "2026-09-23T14:10:00.000Z",
  "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
  "correlationId": "teste-temporal-001",
  "producer": "reserva-service",
  "data": {
    "reservaId": "8f3a2b1c-0d4e-4f5a-9b6c-7d8e9f0a1b2c",
    "clienteId": "33333333-3333-3333-3333-333333333333",
    "sessaoId": "22222222-2222-2222-2222-222222222222",
    "assentos": ["A3"],
    "statusAnterior": "EMITINDO_INGRESSO",
    "status": "CANCELADA"
  }
}
```

---

## Publicação transacional (Transactional Outbox)

O `ReservaApplicationService` grava a reserva e, **na mesma transação JPA**,
os eventos de domínio na tabela `outbox_events` (porta
`EventosDeDominioPublisher`, implementada por `OutboxEventWriter`, que exige
transação aberta: `Propagation.MANDATORY`). Se a transação falhar, nem a
reserva nem o evento são gravados. No fluxo síncrono do CinePass isso é bem
visível: quando a emissão do ingresso falha, o rollback desfaz a reserva **e**
os eventos, e nenhum serviço é avisado de uma reserva que não existe.

O `OutboxPublisher` (`@Scheduled`, a cada 500 ms) publica os pendentes:

1. **Um publicador por vez**: advisory lock do PostgreSQL (`pg_try_advisory_xact_lock`), para que duas instâncias do reserva-service não publiquem eventos da mesma reserva em paralelo.
2. **Ordem de gravação**: `ORDER BY created_at`, esperando a confirmação do broker antes do próximo envio.
3. **Parada na primeira falha**: se um envio falhar, o lote para ali, e os eventos seguintes (possivelmente da mesma reserva) esperam o próximo ciclo.

A linha da outbox guarda também o contexto de tracing da requisição
(`trace_context`), para que o Zipkin mostre um trace único apesar de a
publicação ocorrer em outra thread.

A entrega é *at-least-once*: se o broker confirmar e o processo cair antes do
commit do `PUBLICADO`, o evento sai de novo. Por isso os consumidores são
idempotentes.

## Particionamento, concorrência e ordem

- `reserva.eventos` com 6 partições e chave `reservaId`.
- Cada consumidor roda com 3 threads (`cinepass.kafka.listener.concorrencia`). O Kafka entrega partições inteiras a cada thread: reservas diferentes são processadas em paralelo, e os eventos de uma mesma reserva são processados um de cada vez, em ordem.

## Idempotência

- `notificacao-service` e `fidelidade-service`: tabela `eventos_processados` (PK = `eventId`). O efeito e o registro do `eventId` são gravados na mesma transação; um evento já registrado é ignorado.
- `auditoria-service`: a coluna `event_id` da própria auditoria é `UNIQUE` e faz o papel de guarda.

Como uma partição só é lida por uma thread de cada vez, duas entregas do mesmo
evento chegam sempre em sequência, nunca em paralelo, e a checagem "já
processado?" é suficiente.

## Falhas e dead-letter

Cada consumidor tenta cada mensagem até 4 vezes (1 + 3 retentativas, 1 s de
intervalo). Depois, o `DeadLetterPublishingRecoverer` a envia para o DLT do
serviço, com a mesma chave e com a causa nos headers (`kafka_dlt-exception-*`,
`kafka_dlt-original-*`). As outras partições continuam sendo consumidas
normalmente. Corrigida a causa, as mensagens voltam para o tópico com
`./infra/scripts/reprocessar-dlt.sh <notificacao|fidelidade|auditoria>`.
Reprocessar é seguro graças à idempotência. O script só republica eventos
válidos (com `eventId`); mensagens malformadas, que falhariam de novo em todos
os consumidores, são listadas e ficam no DLT para análise manual.

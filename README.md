# CinePass — Base para ensino de Saga Pattern

Projeto didático para evoluir de uma arquitetura síncrona baseada em DDD para Saga Pattern.

A versão inicial **não implementa Saga, Kafka nem Outbox de propósito**. O objetivo é criar primeiro uma inconsistência distribuída observável e, nas aulas seguintes, evoluir a solução.

> **Assessment — Arquitetura orientada a eventos.** Esta versão do CinePass
> recebeu a evolução pedida no assessment: eventos de domínio da `Reserva`
> publicados no Kafka via **Transactional Outbox**, particionamento por
> `reservaId`, três novos consumidores (**notificação**, **fidelidade** e
> **auditoria**) idempotentes, **retry + dead-letter topic** com
> reprocessamento, **logs centralizados no ELK**, **correlationId** de ponta a
> ponta e **rastreamento distribuído no Zipkin**. Tudo o que foi implementado
> está explicado na seção
> [Assessment: arquitetura orientada a eventos](#assessment-arquitetura-orientada-a-eventos),
> no fim deste README, e em [`docs/`](docs/).

## Stack

- Java 21
- Spring Boot 4.1.1
- Spring Cloud 2025.1.3
- Spring Cloud Netflix Eureka
- Spring Cloud Gateway Server WebFlux
- Spring Cloud LoadBalancer
- Spring Data JPA
- PostgreSQL 17
- Docker Compose
- Temporal (fluxo orquestrado `POST /api/reservas/temporal`)
- Apache Kafka 4.2 (modo KRaft) + Spring for Apache Kafka
- Micrometer Tracing (Brave) + Zipkin
- Elasticsearch, Logstash e Kibana 8.15 (logs centralizados)
- Testcontainers (testes de integração com PostgreSQL e Kafka reais)

> O release train Spring Cloud 2025.1.x suporta Spring Boot 4.1.x e o release 2025.1.3 é compatível com Spring Boot 4.1.x.

---

## Arquitetura inicial

```text
                           CLIENTE
                              |
                              v
                     +----------------+
                     |   API GATEWAY  |
                     |     :8080      |
                     +-------+--------+
                             |
                   Service Discovery
                             |
                      +------v------+
                      |   EUREKA    |
                      |    :8761    |
                      +-------------+

          +------------------+-------------------+
          |                  |                   |
          v                  v                   v
 +----------------+  +----------------+  +----------------+
 | RESERVA        |  | PAGAMENTO      |  | INGRESSO       |
 | SERVICE :8081  |  | SERVICE :8082  |  | SERVICE :8083  |
 +----------------+  +----------------+  +----------------+
 | Filme          |  | Pagamento      |  | Ingresso       |
 | Sessao         |  | Cobranca       |  | Emissao        |
 | Assento        |  | Estorno        |  | Cancelamento   |
 | Reserva        |  |                |  |                |
 +-------+--------+  +-------+--------+  +-------+--------+
         |                   |                   |
         v                   v                   v
   cinepass_db        pagamento_db        ingresso_db
```

O Gateway é a porta de entrada externa. O `reserva-service` usa Eureka + Spring Cloud LoadBalancer para chamar os serviços internos diretamente pelo nome lógico:

```text
http://PAGAMENTO-SERVICE
http://INGRESSO-SERVICE
```

Isso evita transformar o API Gateway em um intermediário obrigatório para chamadas internas.

---

## Estrutura

```text
cinepass-saga-base/
|
|-- discovery-server/
|-- api-gateway/
|-- reserva-service/
|-- pagamento-service/
|-- ingresso-service/
|-- notificacao-service/        (assessment: consumidor Kafka)
|-- fidelidade-service/         (assessment: consumidor Kafka)
|-- auditoria-service/          (assessment: consumidor Kafka)
|-- infra/postgres/init.sql
|-- infra/logstash/pipeline/    (assessment: pipeline do Logstash)
|-- infra/scripts/              (assessment: Kibana, reprocessamento de DLT, evidências)
|-- docs/                       (assessment: arquitetura, eventos, observabilidade, evidências)
|-- requests/cinepass.http
|-- .github/workflows/ci.yml    (assessment: build + testes no GitHub Actions)
|-- docker-compose.yml
`-- pom.xml
```

O `pom.xml` da raiz é apenas um **agregador de build**. Os serviços não compartilham classes de domínio nem dependências entre si.

---

# DDD no projeto

## reserva-service

É o monólito de domínio inicial. Ele concentra:

- Filme
- Sessão
- Assentos
- Reserva

O Aggregate principal é `Reserva`.

```text
Reserva
 |
 |-- ReservaId
 |-- ClienteId
 |-- SessaoId
 |-- AssentoId
 |-- Dinheiro
 |-- StatusReserva
 |-- pagamentoId
 `-- ingressoId
```

Estados:

```text
CRIADA
  |
  v
AGUARDANDO_PAGAMENTO
  |
  v
PAGAMENTO_APROVADO
  |
  v
EMITINDO_INGRESSO
  |
  v
CONFIRMADA
```

O modelo de domínio não depende de JPA. As entidades JPA ficam em `infrastructure.persistence`.

## pagamento-service

Aggregate:

```text
Pagamento
 |
 |-- PagamentoId
 |-- ReservaId
 |-- Dinheiro
 `-- StatusPagamento
```

Estados:

```text
PENDENTE -> APROVADO -> ESTORNADO
        \
         -> RECUSADO
```

O método `estornar()` já existe porque será usado depois como **ação compensatória da Saga**.

## ingresso-service

Aggregate:

```text
Ingresso
 |
 |-- IngressoId
 |-- ReservaId
 |-- SessaoId
 |-- CodigoIngresso
 |-- Assentos
 `-- StatusIngresso
```

Também existe `cancelar()`, que poderá participar de futuras compensações.

---

# O problema didático proposital

O caso de uso `ReservaApplicationService.realizar()` usa uma transação local:

```java
@Transactional
public ReservaDetalhe realizar(...) {
    // reserva assentos no banco local

    // cria reserva no banco local

    pagamentoGateway.cobrar(...); // outro processo + outro banco

    ingressoGateway.emitir(...);  // outro processo + outro banco

    // confirma reserva local
}
```

A anotação `@Transactional` protege apenas `cinepass_db`.

Ela **não** controla as transações dos outros serviços.

## Cenário de falha

```text
1. Reserva assento                 OK
2. Cria Reserva                    OK
3. pagamento-service cobra         OK + COMMIT remoto
4. ingresso-service falha          ERRO
5. reserva-service lança exception
6. cinepass_db faz ROLLBACK
```

Resultado:

```text
cinepass_db
  Reserva: inexistente
  Assento: disponível novamente

pagamento_db
  Pagamento: APROVADO

ingresso_db
  Ingresso: inexistente
```

Esse é o problema que será resolvido posteriormente com Saga.

---

# Como executar

## Opção 1 — tudo com Docker Compose

Na raiz:

```bash
docker compose up --build
```

Serviços:

| Componente | URL |
|---|---|
| API Gateway | http://localhost:8080 |
| Eureka | http://localhost:8761 |
| Reserva Service | http://localhost:8081 |
| Pagamento Service | http://localhost:8082 |
| Ingresso Service | http://localhost:8083 |
| Notificação Service | http://localhost:8084 |
| Fidelidade Service | http://localhost:8085 |
| Auditoria Service | http://localhost:8086 |
| PostgreSQL | localhost:5432 |
| Kafka (acesso externo) | localhost:9092 |
| Kafka UI | http://localhost:8091 |
| Temporal (gRPC) / Temporal UI | localhost:7233 / http://localhost:28081 |
| Zipkin | http://localhost:9411 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |

Na primeira execução o PostgreSQL cria:

```text
cinepass_db
pagamento_db
ingresso_db
notificacao_db
fidelidade_db
auditoria_db
```

(O Temporal cria os próprios bancos, `temporal` e `temporal_visibility`, no
mesmo PostgreSQL.)

Para zerar completamente os bancos:

```bash
docker compose down -v
```

Depois:

```bash
docker compose up --build
```

## Opção 2 — infraestrutura em Docker e aplicações na IDE

Suba somente a infraestrutura (banco, Kafka, Temporal e Zipkin; o ELK é opcional,
porque o envio de logs ao Logstash nunca bloqueia a aplicação):

```bash
docker compose up -d postgres kafka kafka-init temporal zipkin
```

Depois execute, nesta ordem:

```text
DiscoveryServerApplication
PagamentoServiceApplication
IngressoServiceApplication
ReservaServiceApplication
NotificacaoServiceApplication
FidelidadeServiceApplication
AuditoriaServiceApplication
ApiGatewayApplication
```

Os valores padrão dos `application.yml` já apontam para `localhost`
(`localhost:9092` para o Kafka, `localhost:7233` para o Temporal).

---

# Dados iniciais

O `reserva-service` cria automaticamente um filme e uma sessão.

Filme fixo:

```text
11111111-1111-1111-1111-111111111111
```

Sessão fixa:

```text
22222222-2222-2222-2222-222222222222
```

Assentos iniciais:

```text
A1 A2 A3 A4 A5
B1 B2 B3 B4 B5
```

Preço por assento:

```text
R$ 39,90
```

---

# Endpoints pelo Gateway

## Filmes

```http
GET http://localhost:8080/api/filmes
```

## Sessões

```http
GET http://localhost:8080/api/sessoes
```

## Criar reserva

```http
POST http://localhost:8080/api/reservas
Content-Type: application/json

{
  "clienteId": "33333333-3333-3333-3333-333333333333",
  "sessaoId": "22222222-2222-2222-2222-222222222222",
  "assentos": ["A1", "A2"],
  "simularRecusaPagamento": false,
  "simularFalhaIngresso": false
}
```

## Consultar reserva

```http
GET http://localhost:8080/api/reservas/{reservaId}
```

## Consultar pagamento

```http
GET http://localhost:8080/api/pagamentos/{pagamentoId}
```

## Consultar pagamento pela reserva

```http
GET http://localhost:8080/api/pagamentos/reserva/{reservaId}
```

## Estornar pagamento

```http
POST http://localhost:8080/api/pagamentos/{pagamentoId}/estorno
```

## Consultar ingresso pela reserva

```http
GET http://localhost:8080/api/ingressos/reserva/{reservaId}
```

---

# Demonstração 1 — caminho feliz

```http
POST http://localhost:8080/api/reservas
Content-Type: application/json

{
  "clienteId": "33333333-3333-3333-3333-333333333333",
  "sessaoId": "22222222-2222-2222-2222-222222222222",
  "assentos": ["A1"],
  "simularRecusaPagamento": false,
  "simularFalhaIngresso": false
}
```

Resultado esperado:

```text
Reserva    CONFIRMADA
Pagamento APROVADO
Ingresso   EMITIDO
```

---

# Demonstração 2 — pagamento recusado

Use outro assento:

```http
POST http://localhost:8080/api/reservas
Content-Type: application/json

{
  "clienteId": "33333333-3333-3333-3333-333333333333",
  "sessaoId": "22222222-2222-2222-2222-222222222222",
  "assentos": ["A2"],
  "simularRecusaPagamento": true,
  "simularFalhaIngresso": false
}
```

O pagamento será registrado como `RECUSADO` no serviço de pagamento e a transação local do `reserva-service` será revertida.

---

# Demonstração 3 — a falha que introduz Saga

Use outro assento:

```http
POST http://localhost:8080/api/reservas
Content-Type: application/json

{
  "clienteId": "33333333-3333-3333-3333-333333333333",
  "sessaoId": "22222222-2222-2222-2222-222222222222",
  "assentos": ["A3"],
  "simularRecusaPagamento": false,
  "simularFalhaIngresso": true
}
```

A resposta será um erro semelhante a:

```json
{
  "title": "Inconsistência distribuída proposital",
  "status": 500,
  "detail": "O pagamento foi aprovado, mas a emissão do ingresso falhou...",
  "reservaId": "...",
  "pagamentoId": "...",
  "proximoPassoDaAula": "Implementar compensação com Saga Pattern"
}
```

Copie o `pagamentoId` e consulte:

```http
GET http://localhost:8080/api/pagamentos/{pagamentoId}
```

Você verá:

```json
{
  "status": "APROVADO"
}
```

Agora consulte a Reserva usando o `reservaId` retornado no erro:

```http
GET http://localhost:8080/api/reservas/{reservaId}
```

Resultado:

```text
404 Not Found
```

Isso demonstra que:

```text
@Transactional != transação distribuída
```

---

# Evolução planejada para as próximas aulas

## Etapa 1 — compensação síncrona manual

```text
Cobrar
  |
  v
Emitir ingresso
  X
  |
  v
Estornar pagamento
```

## Etapa 2 — Kafka

Trocar parte da comunicação síncrona por eventos.

> As etapas 2 e 3 foram implementadas no assessment para os serviços
> auxiliares (notificação, fidelidade e auditoria); veja a seção
> [Assessment: arquitetura orientada a eventos](#assessment-arquitetura-orientada-a-eventos).

## Etapa 3 — Transactional Outbox

Resolver:

```text
Banco + Kafka
```

## Etapa 4 — Saga Choreography

```text
ReservaCriada
      |
      v
Pagamento
      |
      v
PagamentoAprovado
      |
      v
Ingresso
```

## Etapa 5 — Saga Orchestration

```text
             ReservaSaga
                 |
       +---------+---------+
       |                   |
       v                   v
   Pagamento            Ingresso
```

Depois podem ser adicionados:

- Saga ID
- Commands e Events
- compensações
- idempotência
- retries
- timeouts
- DLT
- observabilidade
- Outbox Pattern

---

# Observação arquitetural importante

Nesta primeira versão, a transação local permanece aberta durante chamadas HTTP externas. Isso é **intencionalmente inadequado para produção** e existe para tornar o problema pedagógico muito claro.

A evolução para Saga irá remover a necessidade de fingir que uma transação ACID local consegue controlar todo o processo distribuído.

---

# Assessment: arquitetura orientada a eventos

O enunciado do assessment descreve um "Marketplace de Freelancers". Os mesmos
conceitos foram aplicados ao domínio do CinePass, com esta correspondência:

| Enunciado | CinePass |
|---|---|
| `contrato-service` / Aggregate Contrato | `reserva-service` / Aggregate `Reserva` |
| `ContratoCriado → EntregaRegistrada → ContratoConcluido` | `ReservaCriada → ReservaPagamentoAprovado → ReservaConfirmada` (+ `ReservaCancelada`) |
| chave `contratoId` | chave `reservaId` |
| `notificacao-service` | `notificacao-service` |
| `reputacao-service` | `fidelidade-service` (histórico e pontos do cliente) |
| `auditoria-service` | `auditoria-service` |

## Visão geral

```text
Cliente ──HTTP──> API Gateway ──> reserva-service ──HTTP──> pagamento-service / ingresso-service
                  (X-Correlation-Id)     │
                                         │ mesma transação: reserva + outbox_events
                                         v
                                  OutboxPublisher (agendado)
                                         │ chave = reservaId
                                         v
                            Kafka: reserva.eventos (6 partições)
                         ┌───────────────┼────────────────┐
                         v               v                v
               notificacao-service  fidelidade-service  auditoria-service
                    (8084)              (8085)              (8086)
                         │ falha após 4 tentativas → reserva.eventos.<servico>.dlt

Todos os serviços → Logstash → Elasticsearch → Kibana   |   traces → Zipkin
```

## 1. Eventos de domínio e Transactional Outbox (reserva-service)

- O Aggregate `Reserva` registra eventos de domínio nas transições de estado
  (`domain/event`): `ReservaCriada`, `ReservaPagamentoAprovado`,
  `ReservaConfirmada` e `ReservaCancelada`. `pullDomainEvents()` entrega e limpa
  a lista.
- O `ReservaApplicationService` salva a reserva e entrega os eventos pendentes
  à porta `EventosDeDominioPublisher`. O adaptador `OutboxEventWriter` grava
  cada evento na tabela `outbox_events`, **na mesma transação** da reserva
  (`Propagation.MANDATORY`). Se a transação fizer rollback (ex.: falha na
  emissão do ingresso), a reserva e os eventos somem juntos.
- O `OutboxPublisher` (`@Scheduled`, 500 ms) lê lotes com
  `FOR UPDATE SKIP LOCKED`, usa um advisory lock do PostgreSQL para ter um único
  publicador por vez, publica de forma síncrona e marca `PUBLICADO`. Na primeira
  falha ele para o lote, para não publicar eventos fora de ordem; a linha fica
  `PENDENTE` e é tentada de novo no próximo ciclo (entrega *at-least-once*).

## 2. Kafka, partições e ordenação

- Tópico `reserva.eventos` com 6 partições; chave da mensagem = `reservaId`.
  Todos os eventos de uma reserva vão para a mesma partição, então são lidos na
  ordem em que foram gravados.
- Cada consumidor usa `concurrency: 3` (3 threads por instância) e
  `AckMode.RECORD`. Reservas diferentes são processadas em paralelo, e a
  mesma reserva nunca fica em duas threads.
- Os tópicos são criados pelo container `kafka-init`; a auto-criação está
  desligada.
- Envelope, payloads e headers: [`docs/EVENTS.md`](docs/EVENTS.md).

## 3. Consumidores e idempotência

| Serviço | O que faz com os eventos | Idempotência |
|---|---|---|
| `notificacao-service` | registra uma notificação para cada evento | tabela `eventos_processados` (PK = `eventId`) |
| `fidelidade-service` | só `ReservaConfirmada`: soma reservas, ingressos, valor gasto e pontos do cliente | tabela `eventos_processados` + `@Version` |
| `auditoria-service` | guarda a trilha completa (payload, partição e offset) | coluna `event_id` única |

O registro do `eventId` e o efeito de negócio ficam na mesma transação local:
uma mensagem entregue duas vezes gera um único efeito.

## 4. Falhas, retry e dead-letter topic

- `DefaultErrorHandler` com `FixedBackOff(1000 ms, 3)`: 1 tentativa + 3
  retentativas. Depois, o `DeadLetterPublishingRecoverer` envia a mensagem para
  `reserva.eventos.<servico>.dlt` (mesma partição e chave), com a exceção nos
  headers `kafka_dlt-*`.
- As outras mensagens continuam sendo consumidas: uma mensagem com problema não
  trava o serviço.
- Reprocessamento, depois de corrigir a causa:

```bash
./infra/scripts/reprocessar-dlt.sh fidelidade
```

O script republica as mensagens da DLT em `reserva.eventos` com a mesma chave.
É seguro porque os consumidores são idempotentes. Mensagens malformadas (que
não são eventos, sem `eventId`) não são republicadas, porque falhariam de novo
em todos os consumidores: o script as lista e elas ficam no DLT para análise.

## 5. Observabilidade

- **Logs**: `logback-spring.xml` em todos os serviços, com saída no console e
  em JSON para o Logstash (assíncrona, não bloqueia a aplicação). Campos:
  `service`, `traceId`, `spanId`, `correlationId`, `reservaId`, `eventId`,
  `eventType`.
- **correlationId**: o Gateway gera ou reaproveita o `X-Correlation-Id`, e ele
  segue pelo MDC do reserva-service, pelas chamadas HTTP para o pagamento e o
  ingresso, pelo envelope e pelo header do evento no Kafka, e pelo MDC dos
  consumidores.
- **ELK**: índice `cinepass-logs-*`. No Kibana, pesquise
  `correlationId.keyword : "<valor>"` para ver a operação inteira.
- **Zipkin**: Micrometer Tracing (Brave), o substituto do Spring Cloud Sleuth
  no Spring Boot 3+. Um único trace cobre gateway → reserva → pagamento /
  ingresso → outbox → Kafka → 3 consumidores.

Detalhes em [`docs/OBSERVABILIDADE.md`](docs/OBSERVABILIDADE.md).

## Como executar a versão do assessment

```bash
docker compose up --build -d
```

Espere os 7 serviços aparecerem no Eureka (http://localhost:8761). O
reserva-service pode reiniciar uma ou duas vezes enquanto o Temporal termina o
setup, o que é esperado. Depois crie o data view do Kibana:

```bash
./infra/scripts/kibana-setup.sh
```

Faça uma reserva com um correlationId próprio:

```bash
curl -i -X POST http://localhost:8080/api/reservas -H "Content-Type: application/json" -H "X-Correlation-Id: demo-001" -d '{"clienteId":"33333333-3333-3333-3333-333333333333","sessaoId":"22222222-2222-2222-2222-222222222222","assentos":["A1","A2"],"simularRecusaPagamento":false,"simularFalhaIngresso":false}'
```

E consulte o resultado nos consumidores:

```http
GET http://localhost:8080/api/notificacoes/reserva/{reservaId}
GET http://localhost:8080/api/fidelidade/33333333-3333-3333-3333-333333333333
GET http://localhost:8080/api/auditoria/reserva/{reservaId}
```

Todas as evidências (API, banco, Kafka, consumo, idempotência, paralelismo,
DLT, ELK, correlationId e Zipkin) podem ser geradas de novo com:

```bash
./infra/scripts/coletar-evidencias.sh
```

O script precisa de um ambiente recém-criado (`docker compose down -v` e
`docker compose up --build -d`), porque usa assentos fixos da sessão de
exemplo. A saída vai para `docs/evidencias/` e está comentada em
[`docs/EVIDENCIAS.md`](docs/EVIDENCIAS.md).

## Testes automatizados

```bash
mvn verify
```

Precisa de Docker, porque os testes de integração sobem PostgreSQL e Kafka
reais com Testcontainers:

| Teste | O que prova |
|---|---|
| `reserva-service` `OutboxReservaIntegrationTest` | os 3 eventos da reserva são publicados em ordem, na mesma partição; se o ingresso falha, nada fica na outbox |
| `notificacao-service` `NotificacaoConsumidorIntegrationTest` | mensagem duplicada gera 1 notificação; mensagem inválida vai para a DLT |
| `fidelidade-service` `IdempotenciaFidelidadeIntegrationTest` | `ReservaConfirmada` duplicada conta uma vez só |
| `fidelidade-service` `ClienteFidelidadeTest` | regra de pontos (unitário) |
| `auditoria-service` `OrdenacaoPorReservaIntegrationTest` | a ordem dos eventos de uma reserva é preservada |

O workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) roda o
`mvn verify` e valida o `docker-compose.yml` a cada push e pull request.

## Mudanças no ambiente em relação à versão inicial

- O `docker-compose.yml` passou a incluir o **Temporal** (antes ficava em
  `docker-compose-temporal.yaml`), porque o reserva-service registra workers na
  inicialização e não sobe sem ele. A Temporal UI fica em http://localhost:28081.
- O Kafka roda em **modo KRaft** (sem Zookeeper), com a imagem `apache/kafka`.
- O Kafka UI está na porta **8091**.
- O PostgreSQL sobe com `max_connections=300`: os 7 serviços com pool Hikari
  mais o Temporal passam do limite padrão de 100 conexões.

## Limitações conhecidas

- O fluxo `POST /api/reservas/temporal` (orquestrado pelo Temporal) também grava
  eventos na outbox, mas as activities rodam fora da requisição HTTP, então
  esses eventos saem sem `correlationId`.
- O fluxo síncrono `POST /api/reservas` continua com o problema didático
  original (pagamento aprovado e ingresso com falha). O Outbox garante que
  **nenhum evento** é publicado nesse caso, mas o estorno do pagamento continua
  sendo tema da Saga.

## Documentação

- [`docs/ARQUITETURA.md`](docs/ARQUITETURA.md): serviços, responsabilidades e decisões.
- [`docs/EVENTS.md`](docs/EVENTS.md): tópicos, eventos, payloads, chaves, DLT e reprocessamento.
- [`docs/OBSERVABILIDADE.md`](docs/OBSERVABILIDADE.md): logs, ELK, correlationId e Zipkin.
- [`docs/EVIDENCIAS.md`](docs/EVIDENCIAS.md): evidências de execução.

#!/usr/bin/env bash
# Executa os cenários da seção "Evidências de execução" e grava a saída de cada
# um em docs/evidencias/*.txt.
#
# Pré-requisito: ambiente RECÉM-CRIADO (os cenários usam assentos da sessão
# inicial, que só tem A1..A5 e B1..B5):
#   docker compose down -v && docker compose up --build -d
#
# Uso (a partir da raiz do projeto):
#   ./infra/scripts/coletar-evidencias.sh
set -uo pipefail
export MSYS_NO_PATHCONV=1

GW="http://localhost:8080"
OUT="docs/evidencias"
mkdir -p "$OUT"
CORR="evidencia-$(date +%Y%m%d%H%M%S)"
SESSAO="22222222-2222-2222-2222-222222222222"
CLIENTE="33333333-3333-3333-3333-$(date +%H%M%S)000000"
SEP='|#|'

psql()  { docker exec cinepass-postgres psql -U cinepass -d "$1" -P pager=off -c "$2"; }
klogs() { docker logs "cinepass-$1" 2>&1; }
secao() { echo; echo "=================================================================="; echo "$*"; echo "=================================================================="; }
reserva_id() { grep -o '"reservaId":"[^"]*"' | head -1 | cut -d'"' -f4; }
reservar() { # assentos-json, correlationId, simularFalhaIngresso
  curl -s -i -X POST "$GW/api/reservas" -H 'Content-Type: application/json' -H "X-Correlation-Id: $2" \
    -d "{\"clienteId\":\"$CLIENTE\",\"sessaoId\":\"$SESSAO\",\"assentos\":$1,\"simularRecusaPagamento\":false,\"simularFalhaIngresso\":${3:-false}}"
}
consumir() { # topico
  docker exec cinepass-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
    --topic "$1" --from-beginning --timeout-ms 8000 \
    --formatter-property print.partition=true --formatter-property print.offset=true \
    --formatter-property print.key=true --formatter-property print.headers=true 2>/dev/null
}
produzir() { # linhas "chave|#|valor" pela entrada padrão
  docker exec -i cinepass-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 \
    --topic reserva.eventos --reader-property parse.key=true --reader-property key.separator="$SEP"
}

# ---------------------------------------------------------------------------
{
secao "AMBIENTE — containers e serviços registrados no Eureka"
docker ps --filter name=cinepass- --format "table {{.Names}}\t{{.Status}}"
echo
curl -s "http://localhost:8761/eureka/apps" -H 'Accept: application/json' | grep -o '"name":"[A-Z-]*"' | sort -u
} > "$OUT/00-ambiente.txt"

# ---------------------------------------------------------------------------
{
secao "1. REQUISIÇÃO RECEBIDA PELO API GATEWAY (correlationId=$CORR)"
echo "\$ curl -i -X POST $GW/api/reservas -H 'X-Correlation-Id: $CORR' -d '{... \"assentos\":[\"A1\",\"A2\"] ...}'"
RESP=$(reservar '["A1","A2"]' "$CORR")
echo "$RESP"
echo "$RESP" | reserva_id > "$OUT/.reserva-id"
sleep 6
echo
echo "--- logs do api-gateway para este correlationId ---"
klogs gateway | grep "$CORR"
} > "$OUT/01-gateway.txt"
RESERVA=$(cat "$OUT/.reserva-id")

# ---------------------------------------------------------------------------
{
secao "2. ALTERAÇÃO PERSISTIDA NO reserva-service (reservaId=$RESERVA)"
psql cinepass_db "SELECT id, status, valor_total, pagamento_id, ingresso_id, criada_em FROM reservas WHERE id='$RESERVA';"
echo "--- outbox: eventos gravados na mesma transação da reserva e depois publicados ---"
psql cinepass_db "SELECT event_type, status, created_at, published_at, correlation_id FROM outbox_events WHERE aggregate_id='$RESERVA' ORDER BY created_at;"
echo "--- logs do reserva-service ---"
klogs reserva | grep "$RESERVA" | grep -E "reserva\.(realizacao|outbox\.registrado|persistida)"
secao "2b. CONSISTÊNCIA: reserva que falha na emissão do ingresso não deixa evento na outbox"
FALHA=$(reservar '["B5"]' "$CORR-falha" true)
echo "$FALHA" | grep -E "^HTTP|title|reservaId"
RF=$(echo "$FALHA" | reserva_id)
echo
psql cinepass_db "SELECT count(*) AS reservas_com_esse_id FROM reservas WHERE id='$RF';"
psql cinepass_db "SELECT count(*) AS eventos_na_outbox FROM outbox_events WHERE aggregate_id='$RF';"
} > "$OUT/02-persistencia-reserva.txt"

# ---------------------------------------------------------------------------
{
secao "3. EVENTOS PUBLICADOS NO KAFKA (tópico reserva.eventos, chave = reservaId)"
klogs reserva | grep "outbox.publicado" | grep "$RESERVA"
echo
echo "--- mensagens lidas do tópico (partição, offset, headers, chave, valor) ---"
consumir reserva.eventos | grep "$RESERVA"
} > "$OUT/03-kafka-publicacao.txt"

# ---------------------------------------------------------------------------
{
secao "4. CONSUMO DOS EVENTOS PELOS SERVIÇOS INTERESSADOS"
for s in notificacao fidelidade auditoria; do
  echo "--- $s-service ---"
  klogs "$s" | grep "$RESERVA" | grep -E "kafka.evento.recebido|processado.sucesso|evento.ignorado"
done
echo
echo "(fidelidade-service só recebe ReservaConfirmada: os demais tipos são descartados pelo filtro por eventType)"
} > "$OUT/04-consumo.txt"

# ---------------------------------------------------------------------------
{
secao "5. PERSISTÊNCIA NOS SERVIÇOS CONSUMIDORES (cada um no seu banco)"
echo "--- notificacao_db ---"
psql notificacao_db "SELECT tipo, destinatario_id, mensagem, criada_em FROM notificacoes WHERE reserva_id='$RESERVA' ORDER BY criada_em;"
echo "--- fidelidade_db ---"
psql fidelidade_db "SELECT cliente_id, reservas_confirmadas, ingressos_comprados, valor_total_gasto, pontos FROM clientes_fidelidade WHERE cliente_id='$CLIENTE';"
echo "--- auditoria_db ---"
psql auditoria_db "SELECT event_type, event_id, correlation_id, occurred_at, recebido_em, particao, offset_kafka FROM auditoria_eventos WHERE reserva_id='$RESERVA' ORDER BY recebido_em;"
echo "--- consulta via API Gateway: GET /api/fidelidade/$CLIENTE ---"
curl -s "$GW/api/fidelidade/$CLIENTE"; echo
echo "--- consulta posterior da auditoria via API Gateway: GET /api/auditoria/reserva/$RESERVA ---"
curl -s "$GW/api/auditoria/reserva/$RESERVA" | sed 's/},{/},\n{/g'; echo
} > "$OUT/05-persistencia-consumidores.txt"

# ---------------------------------------------------------------------------
{
secao "6. MENSAGEM DUPLICADA — reentrega do MESMO ReservaConfirmada (mesmo eventId)"
MSG=$(docker exec cinepass-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic reserva.eventos --from-beginning --timeout-ms 8000 \
  --formatter-property print.key=true --formatter-property key.separator="$SEP" 2>/dev/null \
  | grep -F "$SEP" | grep "$RESERVA" | grep '"ReservaConfirmada"' | head -1)
EVENTO=$(echo "$MSG" | grep -o '"eventId":"[^"]*"' | cut -d'"' -f4)
echo "eventId reenviado: $EVENTO"
echo
echo "ANTES da reentrega:"
psql fidelidade_db  "SELECT reservas_confirmadas, pontos, valor_total_gasto FROM clientes_fidelidade WHERE cliente_id='$CLIENTE';"
psql notificacao_db "SELECT count(*) AS notificacoes_da_reserva FROM notificacoes WHERE reserva_id='$RESERVA';"
psql auditoria_db   "SELECT count(*) AS registros_de_auditoria FROM auditoria_eventos WHERE reserva_id='$RESERVA';"
echo "\$ (republica a mesma mensagem 2x no tópico, com a mesma chave)"
printf '%s\n%s\n' "$MSG" "$MSG" | produzir
sleep 8
echo
echo "DEPOIS da reentrega (os valores devem ser os mesmos):"
psql fidelidade_db  "SELECT reservas_confirmadas, pontos, valor_total_gasto FROM clientes_fidelidade WHERE cliente_id='$CLIENTE';"
psql notificacao_db "SELECT count(*) AS notificacoes_da_reserva FROM notificacoes WHERE reserva_id='$RESERVA';"
psql auditoria_db   "SELECT count(*) AS registros_de_auditoria FROM auditoria_eventos WHERE reserva_id='$RESERVA';"
echo "--- logs: reconhecimento da duplicidade ---"
for s in notificacao fidelidade auditoria; do klogs "$s" | grep "$EVENTO" | grep duplicado; done
} > "$OUT/06-duplicidade.txt"

# ---------------------------------------------------------------------------
{
secao "7. ORDEM POR RESERVA + PROCESSAMENTO CONCORRENTE ENTRE RESERVAS"
# As reservas são feitas uma logo após a outra: todas usam a mesma sessão, e o
# Aggregate Sessao tem controle otimista de versão, então requisições realmente
# simultâneas na mesma sessão são rejeitadas (proteção contra venda duplicada).
# O que interessa aqui é o CONSUMO concorrente, que acontece do mesmo jeito.
echo "Criando 6 reservas em sequência rápida (assentos A3, A4, A5, B1, B2, B3)..."
for a in A3 A4 A5 B1 B2 B3; do
  reservar "[\"$a\"]" "$CORR-ordem-$a" > "/tmp/cp-ordem-$a.txt"
  echo "  $a: $(head -1 "/tmp/cp-ordem-$a.txt")"
done
IDS=$(cat /tmp/cp-ordem-*.txt | grep -o '"reservaId":"[^"]*"' | cut -d'"' -f4 | sort -u)
sleep 8
LISTA=$(printf "'%s'," $IDS); LISTA=${LISTA%,}
echo "--- auditoria: por reserva, a ordem de processamento (recebido_em) segue a ordem de produção (offset) ---"
psql auditoria_db "SELECT reserva_id, event_type, particao, offset_kafka, recebido_em FROM auditoria_eventos WHERE reserva_id IN ($LISTA) ORDER BY reserva_id, recebido_em;"
echo "--- threads do listener (auditoria-service): partições diferentes processadas por threads diferentes ---"
klogs auditoria | grep "kafka.evento.recebido" | grep -E "$(echo $IDS | tr ' ' '|')" \
  | sed -E 's/.*thread=([^ ]+).*partition=([0-9]+).*eventType=([A-Za-z]+) reservaId=([^ ]+).*/thread=\1 partition=\2 \3 reserva=\4/' | sort -u
} > "$OUT/07-ordem-concorrencia.txt"

# ---------------------------------------------------------------------------
{
secao "8. FALHAS: mensagem inválida -> retentativas -> dead-letter topic"
CHAVE="mensagem-invalida-$(date +%s)"
echo "\$ publica em reserva.eventos uma mensagem que não é JSON válido (chave=$CHAVE)"
echo "$CHAVE$SEP{ mensagem corrompida" | produzir
sleep 12
echo "--- notificacao-service: falha registrada, retentativas e envio ao DLT ---"
klogs notificacao | grep -E "kafka.evento.(falha|retry|dlt)" | grep "$CHAVE"
echo
echo "--- mensagem no DLT, com a causa da falha e a origem nos headers ---"
consumir reserva.eventos.notificacao.dlt | tr -d '\000' | tr ',' '\n' \
  | grep -a -E "^Partition:|^kafka_dlt-(exception-cause-fqcn|original-topic|original-consumer-group)|$CHAVE" \
  | sed -E 's/^(Partition:[0-9]+.Offset:[0-9]+).*/\1/; s/^traceparent:[^[:space:]]*[[:space:]]+/chave e valor: /' | head -5
} > "$OUT/08-dlt.txt"

# ---------------------------------------------------------------------------
es_sql() { curl -s "http://localhost:9200/_sql?format=txt" -H 'Content-Type: application/json' -d "{\"query\": \"$1\"}"; }
{
secao "9. LOGS CENTRALIZADOS (Elasticsearch/Kibana) — busca por correlationId=$CORR"
sleep 5
echo "--- linhas de log por serviço para este correlationId ---"
es_sql "SELECT service, COUNT(*) AS linhas FROM \\\"cinepass-logs-*\\\" WHERE correlationId = '$CORR' AND logger_name NOT LIKE 'org.apache.kafka%' GROUP BY service"
echo
echo "--- linhas de log de todos os serviços, em ordem cronológica ---"
es_sql "SELECT \\\"@timestamp\\\", service, SUBSTRING(message, 1, 110) AS message FROM \\\"cinepass-logs-*\\\" WHERE correlationId = '$CORR' AND logger_name NOT LIKE 'org.apache.kafka%' ORDER BY \\\"@timestamp\\\" LIMIT 80"
echo
secao "   busca por reservaId=$RESERVA"
es_sql "SELECT service, COUNT(*) AS linhas FROM \\\"cinepass-logs-*\\\" WHERE reservaId = '$RESERVA' GROUP BY service"
EVT_CONF=$(docker exec cinepass-postgres psql -U cinepass -d cinepass_db -Atc "SELECT id FROM outbox_events WHERE aggregate_id='$RESERVA' AND event_type='ReservaConfirmada'")
secao "   busca por eventId=$EVT_CONF (ReservaConfirmada)"
es_sql "SELECT \\\"@timestamp\\\", service, SUBSTRING(message, 1, 55) AS message FROM \\\"cinepass-logs-*\\\" WHERE eventId = '$EVT_CONF' ORDER BY \\\"@timestamp\\\" LIMIT 20"
} > "$OUT/09-logs-centralizados.txt"

# ---------------------------------------------------------------------------
{
secao "10. PROPAGAÇÃO DO correlationId=$CORR ENTRE OS COMPONENTES"
echo "--- API Gateway ---";                  klogs gateway | grep "$CORR" | grep "request.inicio" | head -1
echo "--- reserva-service (HTTP) ---";        klogs reserva | grep "correlationId=$CORR " | grep "reserva.realizacao.sucesso"
echo "--- pagamento-service (HTTP interno) ---"; klogs pagamento | grep "correlationId=$CORR " | head -2
echo "--- ingresso-service (HTTP interno) ---";  klogs ingresso | grep "correlationId=$CORR " | head -2
echo "--- reserva-service (outbox) ---";      psql cinepass_db "SELECT event_type, correlation_id FROM outbox_events WHERE aggregate_id='$RESERVA' ORDER BY created_at;"
echo "--- Kafka (header + payload) ---";      consumir reserva.eventos | grep "$RESERVA" | grep traceparent | grep -o "correlationId:[^,]*\|\"correlationId\":\"[^\"]*\"" | sort | uniq -c
echo "--- notificacao-service ---";           klogs notificacao | grep "correlationId=$CORR " | grep processado.sucesso
echo "--- fidelidade-service ---";            klogs fidelidade | grep "correlationId=$CORR " | grep processado.sucesso
echo "--- auditoria-service ---";             klogs auditoria | grep "correlationId=$CORR " | grep processado.sucesso
} > "$OUT/10-correlation-id.txt"

# ---------------------------------------------------------------------------
{
secao "11. TRACE NO ZIPKIN"
TRACE=$(klogs reserva | grep "reserva.realizacao.sucesso" | grep "$RESERVA" | grep -o 'traceId=[0-9a-f]*' | head -1 | cut -d= -f2)
echo "traceId da reserva: $TRACE"
echo "$TRACE" > "$OUT/.trace-id"
sleep 3
echo "--- serviços e spans do trace (API do Zipkin: /api/v2/trace/$TRACE) ---"
curl -s "http://localhost:9411/api/v2/trace/$TRACE" \
  | grep -o '"name":"[^"]*","timestamp":[0-9]*,"duration":[0-9]*,"localEndpoint":{"serviceName":"[^"]*"' \
  | sed -E 's/"name":"([^"]*)".*"serviceName":"([^"]*)"/\2  ->  \1/' | sort | uniq -c
echo
echo "Visualização: http://localhost:9411/zipkin/traces/$TRACE"
} > "$OUT/11-zipkin.txt"

# ---------------------------------------------------------------------------
reservar_temporal() { # assento, correlationId, simularFalhaIngresso
  curl -s -i -X POST "$GW/api/reservas/temporal" -H 'Content-Type: application/json' -H "X-Correlation-Id: $2" \
    -d "{\"clienteId\":\"$CLIENTE\",\"sessaoId\":\"$SESSAO\",\"assentos\":[\"$1\"],\"simularRecusaPagamento\":false,\"simularFalhaIngresso\":$3}"
}
{
secao "12. SAGA NO TEMPORAL: correlationId nas activities e compensação (ReservaCancelada)"
echo "\$ POST /api/reservas/temporal  (assento B4, X-Correlation-Id: $CORR-temporal-ok)"
OK=$(reservar_temporal B4 "$CORR-temporal-ok" false)
echo "$OK" | grep -E "^HTTP|^X-Correlation-Id"; echo "$OK" | grep -o '"status":"[A-Z_]*"' | head -1
RT=$(echo "$OK" | reserva_id)
echo
echo "\$ POST /api/reservas/temporal  (assento B5, simularFalhaIngresso=true, X-Correlation-Id: $CORR-temporal-falha)"
FT=$(reservar_temporal B5 "$CORR-temporal-falha" true)
echo "$FT" | grep -E "^HTTP|^X-Correlation-Id"; echo "$FT" | grep -o '"title":"[^"]*"'
RFT=$(echo "$FT" | reserva_id)
sleep 8
echo
echo "--- reserva-service: estado final e eventos gravados na outbox (com o correlationId) ---"
psql cinepass_db "SELECT r.status AS reserva, o.event_type, o.status, o.correlation_id FROM outbox_events o JOIN reservas r ON r.id = o.aggregate_id WHERE o.aggregate_id IN ('$RT','$RFT') ORDER BY o.created_at;"
echo "--- pagamento_db: a compensação estornou o pagamento da reserva que falhou ---"
psql pagamento_db "SELECT reserva_id, status FROM pagamentos WHERE reserva_id IN ('$RT','$RFT') ORDER BY criado_em;"
echo "--- assentos disponíveis na sessão (B5 foi liberado pela compensação) ---"
curl -s "$GW/api/sessoes" | grep -o '"assentosDisponiveis":\[[^]]*\]'
echo
echo "--- consumidores: ReservaCancelada recebido ---"
psql notificacao_db "SELECT tipo, mensagem FROM notificacoes WHERE reserva_id='$RFT' ORDER BY criada_em;"
psql auditoria_db "SELECT event_type, correlation_id FROM auditoria_eventos WHERE reserva_id='$RFT' ORDER BY recebido_em;"
echo "--- logs das activities (reserva-service) e das chamadas ao pagamento com o mesmo correlationId ---"
klogs reserva | grep "correlationId=$CORR-temporal-falha " | grep -E "reserva\.persistida"
klogs pagamento | grep "correlationId=$CORR-temporal-falha "
} > "$OUT/12-temporal-saga.txt"

# ---------------------------------------------------------------------------
{
secao "13. REPROCESSAMENTO: falha transitória (banco fora do ar) -> DLT -> correção -> reprocessamento"
CLI2="44444444-4444-4444-4444-$(date +%H%M%S)000000"
RES2="55555555-5555-5555-5555-$(date +%H%M%S)000000"
EVT2="66666666-6666-6666-6666-$(date +%H%M%S)000000"
echo "\$ docker stop cinepass-postgres   (simula indisponibilidade do banco dos consumidores)"
docker stop cinepass-postgres >/dev/null
echo "\$ publica um ReservaConfirmada (eventId=$EVT2, reservaId=$RES2, clienteId=$CLI2) no tópico"
echo "$RES2$SEP{\"eventId\":\"$EVT2\",\"eventType\":\"ReservaConfirmada\",\"eventVersion\":1,\"occurredAt\":\"2026-01-01T10:00:00Z\",\"reservaId\":\"$RES2\",\"correlationId\":\"$CORR-reprocessamento\",\"producer\":\"reserva-service\",\"data\":{\"reservaId\":\"$RES2\",\"clienteId\":\"$CLI2\",\"assentos\":[\"C1\",\"C2\"],\"valorTotal\":79.80,\"status\":\"CONFIRMADA\"}}" | produzir
sleep 35
echo
echo "--- retentativas e envio ao DLT (fidelidade-service) ---"
klogs fidelidade | grep "$RES2" | grep -E "kafka.evento.(retry|dlt)" | sed -E 's/.*(kafka\.evento\.[a-z.]+.*)/\1/' | cut -c1-200
echo
echo "\$ docker start cinepass-postgres   (causa corrigida)"
docker start cinepass-postgres >/dev/null
until docker exec cinepass-postgres pg_isready -U cinepass >/dev/null 2>&1; do sleep 2; done
sleep 5
echo
echo "ANTES do reprocessamento:"
psql fidelidade_db "SELECT count(*) AS historico_do_cliente FROM clientes_fidelidade WHERE cliente_id='$CLI2';"
echo "\$ ./infra/scripts/reprocessar-dlt.sh fidelidade"
bash infra/scripts/reprocessar-dlt.sh fidelidade
sleep 8
echo
echo "DEPOIS do reprocessamento:"
psql fidelidade_db "SELECT cliente_id, reservas_confirmadas, ingressos_comprados, valor_total_gasto, pontos FROM clientes_fidelidade WHERE cliente_id='$CLI2';"
klogs fidelidade | grep "$EVT2" | grep processado.sucesso | sed -E 's/.*(fidelidade\.evento.*)/\1/'
} > "$OUT/13-reprocessamento.txt"

echo "Evidências gravadas em $OUT/ (correlationId=$CORR, reservaId=$RESERVA)"

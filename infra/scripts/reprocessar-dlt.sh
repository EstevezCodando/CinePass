#!/usr/bin/env bash
# Reprocessa as mensagens do dead-letter topic de um serviço, republicando-as
# no tópico original (reserva.eventos) com a MESMA chave (reservaId), para que
# continuem caindo na mesma partição e preservem a ordem.
#
# Uso (depois de corrigir a causa da falha):
#   ./infra/scripts/reprocessar-dlt.sh notificacao
#   ./infra/scripts/reprocessar-dlt.sh fidelidade
#   ./infra/scripts/reprocessar-dlt.sh auditoria
#
# É seguro reprocessar: todos os consumidores são idempotentes (eventId), então
# serviços que já tinham processado o evento com sucesso simplesmente o ignoram.
# O grupo de consumo "reprocessamento-<servico>" guarda o offset já reprocessado,
# evitando republicar a mesma mensagem da DLT duas vezes.
#
# Mensagens que não são eventos válidos (sem "eventId", ex.: JSON corrompido)
# NÃO são republicadas: reprocessá-las falharia de novo em todos os
# consumidores. Elas continuam no DLT para análise manual e são listadas aqui.
set -euo pipefail

SERVICO="${1:?informe o serviço: notificacao | fidelidade | auditoria}"
DLT="reserva.eventos.${SERVICO}.dlt"
DESTINO="reserva.eventos"
SEP='|#|'
KAFKA_CONTAINER="${KAFKA_CONTAINER:-cinepass-kafka}"

echo "Lendo mensagens pendentes de ${DLT}..."
MENSAGENS=$(docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic "${DLT}" --group "reprocessamento-${SERVICO}" \
  --from-beginning --timeout-ms 20000 \
  --formatter-property print.key=true --formatter-property key.separator="${SEP}" 2>/dev/null \
  | grep -F "${SEP}" || true)

if [ -z "${MENSAGENS}" ]; then
  echo "Nenhuma mensagem pendente em ${DLT}."
  exit 0
fi

INVALIDAS=$(printf '%s\n' "${MENSAGENS}" | grep -vF '"eventId"' || true)
MENSAGENS=$(printf '%s\n' "${MENSAGENS}" | grep -F '"eventId"' || true)
if [ -n "${INVALIDAS}" ]; then
  echo "Ignorando $(printf '%s\n' "${INVALIDAS}" | grep -c .) mensagem(ns) que não são eventos válidos (ficam no DLT para análise):"
  printf '%s\n' "${INVALIDAS}" | awk -v sep="${SEP}" '{ i = index($0, sep); print "  chave=" substr($0, 1, i - 1) }'
fi
if [ -z "${MENSAGENS}" ]; then
  echo "Nenhum evento válido para republicar."
  exit 0
fi

QUANTIDADE=$(printf '%s\n' "${MENSAGENS}" | grep -c . || true)
echo "Republicando ${QUANTIDADE} mensagem(ns) em ${DESTINO}..."
printf '%s\n' "${MENSAGENS}" | docker exec -i "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic "${DESTINO}" \
  --reader-property parse.key=true --reader-property key.separator="${SEP}"
echo "Concluído."

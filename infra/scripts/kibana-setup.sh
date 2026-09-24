#!/usr/bin/env bash
# Cria no Kibana o data view "cinepass-logs-*" usado para consultar os logs
# centralizados de todos os microsserviços. Rode uma vez, depois que o Kibana
# estiver no ar (http://localhost:5601).
set -euo pipefail
KIBANA="${KIBANA_URL:-http://localhost:5601}"

curl -s -X POST "${KIBANA}/api/data_views/data_view" \
  -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
  -d '{"data_view":{"id":"cinepass-logs","title":"cinepass-logs-*","name":"CinePass - logs centralizados","timeFieldName":"@timestamp"},"override":true}' \
  >/dev/null
echo "Data view 'cinepass-logs-*' criado. Abra: ${KIBANA}/app/discover"
echo "Exemplos de consulta (KQL): correlationId.keyword : \"teste-reserva-001\"   |   reservaId.keyword : \"<uuid>\"   |   eventId.keyword : \"<uuid>\""

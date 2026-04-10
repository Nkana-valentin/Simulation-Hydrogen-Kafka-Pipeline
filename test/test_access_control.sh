#!/bin/bash

set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

get_token() {
  local username="$1"
  local password="$2"
  local institution="$3"

  curl -sS -X POST "${API_BASE_URL}/researcher/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"${username}\", \"password\": \"${password}\", \"institution\": \"${institution}\"}" \
    | python3 -c "import sys, json; print(json.load(sys.stdin).get('access_token', ''))"
}

printf "1. Getting dr_smith token: "
TOKEN_SMITH=$(get_token "dr_smith" "research2024" "APSU")
[ -n "${TOKEN_SMITH}" ] && echo "✅" || { echo "❌"; exit 1; }

printf "2. Getting dr_jones token: "
TOKEN_JONES=$(get_token "dr_jones" "hydrogen2024" "MFI")
[ -n "${TOKEN_JONES}" ] && echo "✅" || { echo "❌"; exit 1; }

printf "3. dr_smith tries forbidden temperature sync: "
HTTP_CODE=$(curl -sS -o /tmp/smith_temperature_response.json -w "%{http_code}" \
  -X POST "${API_BASE_URL}/sync/trigger?data_type=temperature&batch_size=10" \
  -H "Authorization: Bearer ${TOKEN_SMITH}")

if [ "${HTTP_CODE}" = "403" ]; then
  echo "✅ (403 Forbidden as expected)"
else
  echo "❌ (Expected 403, got ${HTTP_CODE})"
  cat /tmp/smith_temperature_response.json
  exit 1
fi

printf "4. dr_smith checks sync status (includes available data): "
HTTP_CODE=$(curl -sS -o /tmp/smith_status_response.json -w "%{http_code}" \
  -X GET "${API_BASE_URL}/sync/status" \
  -H "Authorization: Bearer ${TOKEN_SMITH}")

if [ "${HTTP_CODE}" = "200" ]; then
  echo "✅ (200 OK)"
else
  echo "❌ (HTTP ${HTTP_CODE})"
  cat /tmp/smith_status_response.json
  exit 1
fi

printf "5. dr_smith syncs pressure data: "
HTTP_CODE=$(curl -sS -o /tmp/smith_pressure_response.json -w "%{http_code}" \
  -X POST "${API_BASE_URL}/sync/trigger?data_type=pressure&batch_size=100" \
  -H "Authorization: Bearer ${TOKEN_SMITH}")

if [ "${HTTP_CODE}" = "200" ]; then
  echo "✅ (200 OK)"
else
  echo "❌ (HTTP ${HTTP_CODE})"
  cat /tmp/smith_pressure_response.json
  exit 1
fi

printf "6. dr_jones checks sync status (includes available data): "
HTTP_CODE=$(curl -sS -o /tmp/jones_status_response.json -w "%{http_code}" \
  -X GET "${API_BASE_URL}/sync/status" \
  -H "Authorization: Bearer ${TOKEN_JONES}")

if [ "${HTTP_CODE}" = "200" ]; then
  echo "✅ (200 OK)"
else
  echo "❌ (HTTP ${HTTP_CODE})"
  cat /tmp/jones_status_response.json
  exit 1
fi

echo "✅ Access-control checks completed"

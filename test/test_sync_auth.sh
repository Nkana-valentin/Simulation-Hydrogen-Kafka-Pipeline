#!/bin/bash

set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"

extract_json_field() {
  local field="$1"
  python3 -c "import json,sys; data=json.load(sys.stdin); print(data.get('${field}',''))"
}

echo "🔐 Testing Hydrogen Sync Service with Authentication"
echo "===================================================="
echo "Using API base URL: ${API_BASE_URL}"

# Step 1: Check auth service on main API
printf "1. Auth service health: "
HEALTH=$(curl -sS "${API_BASE_URL}/health" | extract_json_field "status")
echo "${HEALTH}"

if [ "${HEALTH}" != "healthy" ]; then
  echo "❌ Auth service not healthy at ${API_BASE_URL}/health"
  exit 1
fi

# Step 2: Get researcher token
printf "2. Getting researcher token: "
TOKEN=$(curl -sS -X POST "${API_BASE_URL}/researcher/login" \
  -H "Content-Type: application/json" \
  -d '{"username": "dr_smith", "password": "research2024", "institution": "APSU"}' \
  | python3 -c "import sys, json; print(json.load(sys.stdin).get('access_token', ''))")

if [ -z "${TOKEN}" ]; then
  echo "❌ Failed to get researcher token"
  exit 1
fi

echo "✅ (token: ${TOKEN:0:20}...)"

# Step 3: Trigger sync
printf "3. Triggering data sync: "
HTTP_CODE=$(curl -sS -o /tmp/sync_trigger_response.json -w "%{http_code}" \
  -X POST "${API_BASE_URL}/sync/trigger" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json")

if [ "${HTTP_CODE}" = "200" ]; then
  echo "✅ (200 OK)"
else
  echo "❌ (HTTP ${HTTP_CODE})"
  cat /tmp/sync_trigger_response.json
  exit 1
fi

# Step 4: Check sync status
printf "4. Checking sync status: "
HTTP_CODE=$(curl -sS -o /tmp/sync_status_response.json -w "%{http_code}" \
  -X GET "${API_BASE_URL}/sync/status" \
  -H "Authorization: Bearer ${TOKEN}")

if [ "${HTTP_CODE}" = "200" ]; then
  echo "✅ (200 OK)"
else
  echo "❌ (HTTP ${HTTP_CODE})"
  cat /tmp/sync_status_response.json
  exit 1
fi

echo "===================================================="
echo "✅ Test complete!"

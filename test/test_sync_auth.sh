#!/bin/bash

echo "🔐 Testing Hydrogen Sync Service with Authentication"
echo "===================================================="

# Step 1: Check auth service
echo -n "1. Auth service health: "
HEALTH=$(curl -s http://localhost:8001/auth/health | python3 -c "import sys, json; print(json.load(sys.stdin).get('status', 'error'))")
echo "$HEALTH"

if [ "$HEALTH" != "healthy" ]; then
    echo "❌ Auth service not running. Start it with: python -m auth_service.auth_server"
    exit 1
fi

# Step 2: Get researcher token
echo -n "2. Getting researcher token: "
TOKEN=$(curl -s -X POST http://localhost:8001/auth/researcher/login \
  -H "Content-Type: application/json" \
  -d '{"username": "dr_smith", "password": "research2024", "institution": "APSU"}' \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")
echo "✅ (token: ${TOKEN:0:20}...)"

# Step 3: Test debug endpoint
echo -n "3. Testing token with debug endpoint: "
RESPONSE=$(curl -s -w "%{http_code}" -X GET http://localhost:8080/api/debug/token-info \
  -H "Authorization: Bearer $TOKEN")
HTTP_CODE="${RESPONSE: -3}"
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ (200 OK)"
else
    echo "❌ (HTTP $HTTP_CODE)"
fi

# Step 4: Trigger sync
echo -n "4. Triggering data sync: "
RESPONSE=$(curl -s -w "%{http_code}" -X POST http://localhost:8080/api/sync/trigger \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json")
HTTP_CODE="${RESPONSE: -3}"
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ (200 OK)"
else
    echo "❌ (HTTP $HTTP_CODE)"
fi

# Step 5: Check sync status
echo -n "5. Checking sync status: "
RESPONSE=$(curl -s -w "%{http_code}" -X GET http://localhost:8080/api/sync/status \
  -H "Authorization: Bearer $TOKEN")
HTTP_CODE="${RESPONSE: -3}"
if [ "$HTTP_CODE" = "200" ]; then
    echo "✅ (200 OK)"
else
    echo "❌ (HTTP $HTTP_CODE)"
fi

echo "===================================================="
echo "✅ Test complete!"

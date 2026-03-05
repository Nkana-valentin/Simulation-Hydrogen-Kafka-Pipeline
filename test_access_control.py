# 1. Get dr_smith token (pressure, flow)
TOKEN_SMITH=$(curl -s -X POST http://localhost:8001/auth/researcher/login \
  -H "Content-Type: application/json" \
  -d '{"username": "dr_smith", "password": "research2024", "institution": "APSU"}' \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

# 2. Get dr_jones token (temperature, voltage)
TOKEN_JONES=$(curl -s -X POST http://localhost:8001/auth/researcher/login \
  -H "Content-Type: application/json" \
  -d '{"username": "dr_jones", "password": "hydrogen2024", "institution": "MFI"}' \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

# 3. dr_smith tries to access temperature data (SHOULD FAIL)
curl -X POST "http://localhost:8080/api/sync/trigger?data_type=temperature&batch_size=10" \
  -H "Authorization: Bearer $TOKEN_SMITH"

# Expected: 403 Forbidden - "Access denied: temperature not in your permissions (pressure, flow)"

# 4. dr_smith checks available data
curl -X GET "http://localhost:8080/api/sync/available-data" \
  -H "Authorization: Bearer $TOKEN_SMITH"

# Expected: Shows only pressure and flow counts

# 5. dr_smith syncs pressure data (SHOULD WORK)
curl -X POST "http://localhost:8080/api/sync/trigger?data_type=pressure&batch_size=100" \
  -H "Authorization: Bearer $TOKEN_SMITH"

# 6. dr_jones checks their available data
curl -X GET "http://localhost:8080/api/sync/available-data" \
  -H "Authorization: Bearer $TOKEN_JONES"

# Expected: Shows only temperature and voltage counts

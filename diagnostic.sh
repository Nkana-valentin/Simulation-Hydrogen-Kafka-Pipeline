#!/bin/bash

set -e

echo "=============================="
echo "🚀 SYSTEM DIAGNOSTIC START"
echo "=============================="

# -----------------------------------
# Helper functions
# -----------------------------------
check() {
  if eval "$1"; then
    echo "✅ OK"
  else
    echo "❌ FAIL"
  fi
}

section() {
  echo -e "\n=============================="
  echo "🔎 $1"
  echo "=============================="
}

# -----------------------------------
# 1. Containers
# -----------------------------------
section "1. Running containers"
docker ps

section "2. All containers"
docker ps -a

section "3. Health status"
docker ps --format "table {{.Names}}\t{{.Status}}"

# -----------------------------------
# 2. Logs
# -----------------------------------
section "4. Logs (fastapi_app)"
docker logs fastapi_app --tail 20 || true

section "5. Logs (kafka_producer)"
docker logs kafka_producer --tail 20 || true

section "6. Logs (kafka_consumer)"
docker logs kafka_consumer --tail 20 || true

section "7. Logs (broker)"
docker logs broker --tail 20 || true

section "8. Logs (questdb)"
docker logs custom_questdb --tail 20 || true

# -----------------------------------
# 3. Environment check
# -----------------------------------
section "9. Environment inside FastAPI"
docker exec fastapi_app env | grep -E "QUESTDB|KAFKA" || true

# -----------------------------------
# 4. Network debugging (CRITICAL)
# -----------------------------------
section "10. DNS resolution inside FastAPI"

echo -n "QuestDB DNS → "
check "docker exec fastapi_app getent hosts questdb"

echo -n "Kafka DNS → "
check "docker exec fastapi_app getent hosts broker"

# -----------------------------------
# 5. Connectivity tests
# -----------------------------------
section "11. Connectivity tests"

echo -n "FastAPI → QuestDB HTTP → "
check "docker exec fastapi_app python -c \"import requests; r=requests.get('http://questdb:9000/exec?query=SELECT+1'); exit(0 if r.status_code==200 else 1)\""

echo -n "FastAPI → Kafka TCP → "
check "docker exec fastapi_app bash -c 'echo > /dev/tcp/broker/9092'"

# -----------------------------------
# 6. Kafka checks
# -----------------------------------
section "12. Kafka status"

echo -n "List topics → "
check "docker exec broker kafka-topics --bootstrap-server localhost:9092 --list"

echo -n "Check topic raw_h2_data → "
check "docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe --topic raw_h2_data"

# -----------------------------------
# 7. QuestDB checks
# -----------------------------------
section "13. QuestDB checks"

echo -n "Query SELECT 1 → "
check "docker exec fastapi_app python -c \"import requests; r=requests.get('http://questdb:9000/exec?query=SELECT+1'); exit(0 if r.status_code==200 else 1)\""

echo -e "\nRow count:"
docker exec fastapi_app python -c "import requests; print(requests.get('http://questdb:9000/exec?query=SELECT+count(*)+FROM+raw_h2_data').text)" || true

echo -e "\nTables:"
docker exec fastapi_app python -c "import requests; print(requests.get('http://questdb:9000/exec?query=SHOW+TABLES').text)" || true

# -----------------------------------
# 8. Volume check
# -----------------------------------
section "14. QuestDB data directory"
docker exec custom_questdb ls /var/lib/questdb || true

# -----------------------------------
# 9. Resource usage
# -----------------------------------
section "15. Resource usage"
docker stats --no-stream

# -----------------------------------
# 10. Final summary
# -----------------------------------
echo -e "\n=============================="
echo "✅ DIAGNOSTIC COMPLETE"
echo "=============================="
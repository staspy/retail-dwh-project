#!/usr/bin/env bash
# Retail DWH: полный идемпотентный сетап (сеть/NAT/SG/SA/бакеты/ClickHouse/Airflow/creds.env)
set -u
C_ERR='\033[0;31m'; C_WARN='\033[1;33m'; C_END='\033[0m'
info(){ echo "== $1 =="; }
warn(){ echo -e "${C_WARN}WARN: $1${C_END}"; }
die(){ echo -e "${C_ERR}ERROR: $1${C_END}"; exit 1; }
jstr(){ grep -o "\"$1\": \"[^\"]*\"" | head -1 | cut -d'"' -f4; }
genpw(){ echo "Aa1-$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 10)"; }
RAND=$(head -c6 /dev/urandom | od -An -tx1 | tr -d ' \n')

# Инициализация переменных для защиты от set -u
STAGING_BUCKET=""
LOGS_BUCKET=""
DAGS_BUCKET=""
S3_ACCESS_KEY=""
S3_SECRET_KEY=""
CH_ETL_PASSWORD=""
CH_ANALYST_PASSWORD=""
CH_FQDN=""
AIRFLOW_ADMIN_PASSWORD=""

info "0/9 Каталог"
FOLDER_ID=$(yc config get folder-id 2>/dev/null)
[ -n "${FOLDER_ID}" ] || FOLDER_ID=$(yc resource-manager folder list --format json 2>/dev/null | jstr id)
[ -n "${FOLDER_ID}" ] || die "каталог не определён: выполните 'yc init'"
echo "Каталог: ${FOLDER_ID}"
[ -f creds.env ] && . ./creds.env

info "1/9 Сеть и подсеть (переиспользуем существующие, квоту не трогаем)"
SUBNET_ID=$(yc vpc subnet get retail-subnet --format json 2>/dev/null | jstr id)
[ -n "${SUBNET_ID}" ] || SUBNET_ID=$(yc vpc subnet list --folder-id="${FOLDER_ID}" --format json 2>/dev/null | jstr id)
if [ -n "${SUBNET_ID}" ]; then
  echo "Переиспользуем существующую подсеть: ${SUBNET_ID}"
else
  NET_ID=$(yc vpc network create retail-net --format json 2>/dev/null | jstr id) \
    || die "сеть не создана: квота vpc.networks.count исчерпана — удалите лишнюю сеть: yc vpc network list; yc vpc network delete <id>"
  SUBNET_ID=$(yc vpc subnet create retail-subnet --network-id="${NET_ID}" --zone ru-central1-a --range 10.128.0.0/24 --format json | jstr id) \
    || die "подсеть не создана"
fi
NET_ID=$(yc vpc subnet get "${SUBNET_ID}" --format json | jstr network_id)
ZONE=$(yc vpc subnet get "${SUBNET_ID}" --format json | jstr zone_id)
echo "Сеть: ${NET_ID} | Подсеть: ${SUBNET_ID} | Зона: ${ZONE}"

info "2/9 NAT-шлюз и таблица маршрутов"
GATEWAY_ID=$(yc vpc gateway get retail-nat --format json 2>/dev/null | jstr id)
[ -n "${GATEWAY_ID}" ] || GATEWAY_ID=$(yc vpc gateway create --name retail-nat --format json 2>/tmp/gw.log | jstr id) || true
if [ -n "${GATEWAY_ID}" ]; then
  RT_ID=$(yc vpc route-table get retail-rt --format json 2>/dev/null | jstr id)
  if [ -n "${RT_ID}" ] && [ "$(yc vpc route-table get "${RT_ID}" --format json | jstr network_id)" != "${NET_ID}" ]; then
    RT_ID=""
  fi
  [ -n "${RT_ID}" ] || RT_ID=$(yc vpc route-table create retail-rt --network-id="${NET_ID}" \
    --route "destination=0.0.0.0/0,gateway-id=${GATEWAY_ID}" --format json 2>/tmp/rt.log | jstr id) || true
  if [ -n "${RT_ID}" ]; then
    yc vpc subnet update "${SUBNET_ID}" --route-table-id="${RT_ID}" >/dev/null 2>&1 \
      && echo "NAT ${GATEWAY_ID} и таблица ${RT_ID} привязаны к подсети" \
      || warn "таблица маршрутов не привязалась к подсети"
  else
    warn "таблица маршрутов не создана: $(tail -1 /tmp/rt.log | tr '\n' ' ')"
  fi
else
  warn "NAT-шлюз не создан: $(tail -1 /tmp/gw.log | tr '\n' ' ') — исходящий трафик из приватной подсети может не работать"
fi

info "3/9 Группа безопасности"
SG_ID=$(yc vpc security-group get retail-sg --format json 2>/dev/null | jstr id)
if [ -z "${SG_ID}" ]; then
  SG_ID=$(yc vpc security-group create retail-sg --network-id="${NET_ID}" \
    --rule "direction=ingress,protocol=any,from-port=0,to-port=65535,predefined=self_security_group" \
    --rule "direction=ingress,protocol=tcp,port=8443,v4-cidrs=[0.0.0.0/0]" \
    --rule "direction=ingress,protocol=tcp,port=9440,v4-cidrs=[0.0.0.0/0]" \
    --rule "direction=egress,protocol=any,from-port=0,to-port=65535,v4-cidrs=[0.0.0.0/0]" \
    --format json 2>/tmp/sg.log | jstr id) || true
  [ -n "${SG_ID}" ] || warn "группа безопасности не создана: $(tail -1 /tmp/sg.log | tr '\n' ' ') — кластеры создадутся с группой по умолчанию"
fi
echo "SG: ${SG_ID:-группа по умолчанию}"

info "4/9 Сервисные аккаунты и роли"
SA_S3_ID=$(yc iam service-account get s3-writer --format json 2>/dev/null | jstr id)
[ -n "${SA_S3_ID}" ] || SA_S3_ID=$(yc iam service-account create --name s3-writer --format json | jstr id) || die "не создан s3-writer"
SA_AF_ID=$(yc iam service-account get airflow-agent --format json 2>/dev/null | jstr id)
[ -n "${SA_AF_ID}" ] || SA_AF_ID=$(yc iam service-account create --name airflow-agent --format json | jstr id) || die "не создан airflow-agent"
bind(){ yc resource-manager folder add-access-binding "${FOLDER_ID}" --role "$1" --service-account-id "$2" >/dev/null 2>&1 || true; }
bind storage.uploader "${SA_S3_ID}"
bind storage.editor "${SA_AF_ID}"
bind managed-airflow.integrationProvider "${SA_AF_ID}"
echo "SA: s3-writer=${SA_S3_ID} | airflow-agent=${SA_AF_ID}"

info "5/9 Бакеты"
if [ -z "${STAGING_BUCKET:-}" ]; then
  STAGING_BUCKET=$(yc storage bucket list --format json 2>/dev/null | grep -o '"name": "retail-staging-[^"]*"' | head -1 | cut -d'"' -f4)
fi
[ -n "${STAGING_BUCKET}" ] || { STAGING_BUCKET="retail-staging-${RAND}"; yc storage bucket create --name "${STAGING_BUCKET}" --default-storage-class standard >/dev/null 2>&1 || die "бакет staging не создан"; }
if [ -z "${LOGS_BUCKET:-}" ]; then
  LOGS_BUCKET=$(yc storage bucket list --format json 2>/dev/null | grep -o '"name": "retail-logs-[^"]*"' | head -1 | cut -d'"' -f4)
fi
[ -n "${LOGS_BUCKET}" ] || { LOGS_BUCKET="retail-logs-${RAND}"; yc storage bucket create --name "${LOGS_BUCKET}" --default-storage-class standard >/dev/null 2>&1 || die "бакет logs не создан"; }
if [ -z "${DAGS_BUCKET:-}" ]; then
  DAGS_BUCKET=$(yc storage bucket list --format json 2>/dev/null | grep -o '"name": "retail-dags-[^"]*"' | head -1 | cut -d'"' -f4)
fi
[ -n "${DAGS_BUCKET}" ] || { DAGS_BUCKET="retail-dags-${RAND}"; yc storage bucket create --name "${DAGS_BUCKET}" --default-storage-class standard >/dev/null 2>&1 || die "бакет dags не создан"; }
echo "Staging: ${STAGING_BUCKET} | Логи: ${LOGS_BUCKET} | DAGs: ${DAGS_BUCKET}"

info "6/9 Статический ключ S3"
if [ -z "${S3_ACCESS_KEY:-}" ]; then
  KEY_JSON=$(yc iam access-key create --service-account-id "${SA_S3_ID}" --format json 2>/tmp/key.log)
  S3_ACCESS_KEY=$(echo "${KEY_JSON}" | jstr key_id)
  S3_SECRET_KEY=$(echo "${KEY_JSON}" | jstr secret)
  [ -n "${S3_ACCESS_KEY}" ] && [ -n "${S3_SECRET_KEY}" ] || die "ключ S3 не создан: $(tail -1 /tmp/key.log | tr '\n' ' ')"
fi
echo "Ключ: ${S3_ACCESS_KEY}"

info "7/9 Кластер ClickHouse retail-ch"
if yc managed-clickhouse cluster get retail-ch >/dev/null 2>&1; then
  echo "Кластер retail-ch уже есть"
else
  CH_ETL_PASSWORD="${CH_ETL_PASSWORD:-$(genpw)}"
  CH_ANALYST_PASSWORD="${CH_ANALYST_PASSWORD:-$(genpw)}"
  PRESETS=$(yc managed-clickhouse resource-preset list --format json 2>/dev/null | grep -o '"id": "[^"]*"' | cut -d'"' -f4 | tr '\n' ' ')
  CREATED=0
  for P in s3-c2-m8 ${PRESETS}; do
    [ -n "${P}" ] || continue
    echo "Попытка создания кластера с классом ${P}..."
    if yc managed-clickhouse cluster create retail-ch \
      --environment production \
      --network-id "${NET_ID}" \
      --host type=clickhouse,zone-id="${ZONE}",subnet-id="${SUBNET_ID}" \
      --clickhouse-resource-preset "${P}" \
      --clickhouse-disk-type network-ssd --clickhouse-disk-size 20 \
      --database name=dwh \
      --user name=etl_user,password="${CH_ETL_PASSWORD}" \
      --user name=analyst,password="${CH_ANALYST_PASSWORD}" \
      ${SG_ID:+--security-group-ids "${SG_ID}"} \
      --websql-access --datalens-access >/tmp/ch.log 2>&1; then
      CREATED=1; break
    fi
    echo "  не удалось: $(tail -2 /tmp/ch.log | tr '\n' ' ')"
  done
  [ "${CREATED}" = "1" ] || die "кластер ClickHouse не создан. Лог: $(tail -3 /tmp/ch.log | tr '\n' ' ')"
fi

echo "Ждём статус RUNNING ClickHouse..."
ST=""
for i in $(seq 1 60); do
  ST=$(yc managed-clickhouse cluster get retail-ch --format json 2>/dev/null | jstr status)
  [ "${ST}" = "RUNNING" ] && break
  sleep 20
done
[ "${ST}" = "RUNNING" ] || warn "ClickHouse пока не RUNNING (статус: ${ST:-?}) — подождите и перезапустите скрипт"

# --- БЛОК ПОЛЬЗОВАТЕЛЕЙ, БАЗ ДАННЫХ И ПРАВ ---
info "Создание баз данных и настройка прав"

# 1. Установка паролей (если нужно)
if [ -z "${CH_ETL_PASSWORD:-}" ] || [ -z "${CH_ANALYST_PASSWORD:-}" ]; then
  CH_ETL_PASSWORD="${CH_ETL_PASSWORD:-$(genpw)}"
  CH_ANALYST_PASSWORD="${CH_ANALYST_PASSWORD:-$(genpw)}"
  for U in etl_user analyst; do
    if [ "${U}" = "etl_user" ]; then PW="${CH_ETL_PASSWORD}"; else PW="${CH_ANALYST_PASSWORD}"; fi
    OK=0
    for TRY in 1 2 3; do
      if yc managed-clickhouse user update "${U}" --cluster-name retail-ch --password "${PW}" >/dev/null 2>&1; then
        OK=1; break
      fi
      echo "  ${U}: смена пароля, попытка ${TRY} не прошла, ждём 20 сек..."
      sleep 20
    done
    [ "${OK}" = "1" ] && echo "  ${U}: пароль обновлён" || warn "пароль ${U} не обновился"
  done
fi

# 2. Создание БД raw (если её нет)
echo "Проверка базы данных raw..."
if ! yc managed-clickhouse database list --cluster-name retail-ch --format json 2>/dev/null | grep -q '"name": "raw"'; then
  echo "  База raw не найдена, создаём..."
  yc managed-clickhouse database create raw --cluster-name retail-ch >/dev/null 2>&1 || warn "Не удалось создать БД raw"
else
  echo "  База raw уже существует."
fi

# 3. Пауза для завершения фоновых операций
echo "Ожидание завершения предыдущих операций с пользователями..."
sleep 30

# 4. Выдача прав etl_user на dwh и raw
for DB in dwh raw; do
  echo "Выдача прав etl_user на ${DB}..."
  GRANT_OK=0
  for TRY in 1 2 3 4 5; do
    GRANT_OUT=$(yc managed-clickhouse user grant-permission etl_user --cluster-name retail-ch --database "${DB}" 2>&1)
    GRANT_EXIT=$?

    if [ $GRANT_EXIT -eq 0 ]; then
      echo "  Права etl_user на ${DB} выданы успешно."
      GRANT_OK=1; break
    elif echo "$GRANT_OUT" | grep -q "already has access"; then
      echo "  Права etl_user на ${DB} уже были выданы ранее."
      GRANT_OK=1; break
    else
      echo "  Попытка ${TRY}: $(echo "$GRANT_OUT" | head -1)"
    fi
    sleep 20
  done
  if [ "$GRANT_OK" != "1" ]; then
    warn "Не удалось выдать права etl_user на ${DB}."
  fi
done

# 5. Выдача прав analyst только на dwh
echo "Выдача прав analyst на dwh..."
GRANT_OK=0
for TRY in 1 2 3 4 5; do
  GRANT_OUT=$(yc managed-clickhouse user grant-permission analyst --cluster-name retail-ch --database dwh 2>&1)
  GRANT_EXIT=$?

  if [ $GRANT_EXIT -eq 0 ]; then
    echo "  Права analyst на dwh выданы успешно."
    GRANT_OK=1; break
  elif echo "$GRANT_OUT" | grep -q "already has access"; then
    echo "  Права analyst на dwh уже были выданы ранее."
    GRANT_OK=1; break
  else
    echo "  Попытка ${TRY}: $(echo "$GRANT_OUT" | head -1)"
  fi
  sleep 20
done
if [ "$GRANT_OK" != "1" ]; then
  warn "Не удалось выдать права analyst на dwh."
fi
# ---------------------------------------------

CH_FQDN=$(yc managed-clickhouse host list --cluster-name retail-ch --format json 2>/dev/null | grep -o '"name": "[^"]*"' | head -1 | cut -d'"' -f4)
[ -n "${CH_FQDN}" ] || CH_FQDN=$(yc managed-clickhouse host list --cluster-name retail-ch 2>/dev/null | awk 'NR==2{print $1}')
[ -n "${CH_FQDN}" ] || warn "FQDN не найден — посмотрите в консоли: retail-ch → Хосты и впишите в creds.env"
echo "ClickHouse FQDN: ${CH_FQDN:-?}"

info "8/9 Кластер Airflow retail-airflow"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-$(genpw)}"
if yc managed-airflow cluster get retail-airflow >/dev/null 2>&1; then
  echo "Кластер retail-airflow уже есть"
else
  SPEC="--webserver count=1,resource-preset-id=c1-m2 --scheduler count=1,resource-preset-id=c1-m2 --dag-processor count=1,resource-preset-id=c1-m2 --worker min-count=1,max-count=4,resource-preset-id=c1-m2"
  if ! yc managed-airflow cluster create retail-airflow \
      --admin-password "${AIRFLOW_ADMIN_PASSWORD}" \
      --service-account-id "${SA_AF_ID}" \
      --subnet-ids "${SUBNET_ID}" \
      ${SG_ID:+--security-group-ids "${SG_ID}"} \
      --dags-bucket "${DAGS_BUCKET}" \
      ${SPEC} >/tmp/af.log 2>&1; then
    echo "  попытка 1 не удалась: $(tail -2 /tmp/af.log | tr '\n' ' ')"
    die "кластер Airflow не создан — пришлите вывод выше"
  fi
  echo "Кластер Airflow создан"
fi
echo "Ждём статус RUNNING Airflow (10–20 минут)..."
ST=""
for i in $(seq 1 90); do
  ST=$(yc managed-airflow cluster get retail-airflow --format json 2>/dev/null | jstr status)
  [ "${ST}" = "RUNNING" ] && break
  sleep 20
done
[ "${ST}" = "RUNNING" ] || warn "Airflow пока не RUNNING (статус: ${ST:-?}) — можно перезапустить скрипт позже"

info "9/9 creds.env"
AF_URL=$(yc managed-airflow cluster get retail-airflow --format json 2>/dev/null | grep -oiE '"web[_-]?server[_-]?url": "[^"]*"' | head -1 | cut -d'"' -f4)
[ -n "${AF_URL}" ] || AF_URL=$(yc managed-airflow cluster get retail-airflow --format json 2>/dev/null | grep -oE 'https://[a-zA-Z0-9.-]+' | head -1)
cat > creds.env <<ENV
export FOLDER_ID='${FOLDER_ID}'
export NET_ID='${NET_ID}'
export SUBNET_ID='${SUBNET_ID}'
export ZONE='${ZONE}'
export SG_ID='${SG_ID}'
export SA_S3_ID='${SA_S3_ID}'
export SA_AF_ID='${SA_AF_ID}'
export STAGING_BUCKET='${STAGING_BUCKET}'
export LOGS_BUCKET='${LOGS_BUCKET}'
export DAGS_BUCKET='${DAGS_BUCKET}'
export S3_ACCESS_KEY='${S3_ACCESS_KEY}'
export S3_SECRET_KEY='${S3_SECRET_KEY}'
export CH_ETL_PASSWORD='${CH_ETL_PASSWORD}'
export CH_ANALYST_PASSWORD='${CH_ANALYST_PASSWORD}'
export CH_FQDN='${CH_FQDN}'
export AIRFLOW_ADMIN_PASSWORD='${AIRFLOW_ADMIN_PASSWORD}'
export AIRFLOW_URL='${AF_URL}'
ENV

if [ -f creds.env ]; then
  chmod 600 creds.env 2>/dev/null || true
fi

echo "Готово. Всё в creds.env:"
cat creds.env
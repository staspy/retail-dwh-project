#!/usr/bin/env bash

# ===== Деплой трёх функций-генераторов данных и таймер-триггеров =====
# Запускается из корня проекта после 00_setup_infra.sh (нужен creds.env)

if [ ! -f creds.env ]; then
    echo "ERROR: Файл creds.env не найден. Запустите 00_setup_infra.sh или проверьте, что вы в корне проекта."
    exit 1
fi
source ./creds.env

# Имя сервисного аккаунта, от которого будут создаваться триггеры.
TRIGGER_SA_NAME="trigger-creator"

deploy() {
  NAME=$1
  if [ -d "functions/$NAME" ]; then
    DIR="functions/$NAME"
  elif [ -d "functions/${NAME//-/_}" ]; then
    DIR="functions/${NAME//-/_}"
  else
    echo "ERROR: Не найдена папка для функции $NAME в директории functions/."
    exit 1
  fi

  echo "== Функция $NAME =="
  (cd "$DIR" && zip -q -r "../$NAME.zip" index.py requirements.txt) || {
    echo "ERROR: Не удалось создать архив $NAME.zip"
    exit 1
  }

  yc serverless function create --name "$NAME" > /dev/null 2>&1
  
  yc serverless function version create \
    --function-name "$NAME" \
    --runtime python312 \
    --entrypoint index.handler \
    --memory 128m \
    --execution-timeout 30s \
    --service-account-id "$SA_S3_ID" \
    --environment "AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY,AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY,S3_BUCKET=$STAGING_BUCKET,S3_ENDPOINT=https://storage.yandexcloud.net" \
    --source-path "functions/$NAME.zip" > /dev/null
  
  rm -f "functions/$NAME.zip"
}

deploy gen-pos-sales
deploy gen-loyalty
deploy gen-online-orders

echo ""
echo "== Подготовка к созданию триггеров =="

echo "Проверка сервисного аккаунта $TRIGGER_SA_NAME..."
TRIGGER_SA_ID=$(yc iam service-account get --name "$TRIGGER_SA_NAME" --format json 2>/dev/null | grep -o '"id": "[^"]*"' | head -1 | cut -d'"' -f4)

if [ -z "$TRIGGER_SA_ID" ]; then
  echo "  Аккаунт не найден, создаем..."
  yc iam service-account create --name "$TRIGGER_SA_NAME" > /dev/null
  TRIGGER_SA_ID=$(yc iam service-account get --name "$TRIGGER_SA_NAME" --format json 2>/dev/null | grep -o '"id": "[^"]*"' | head -1 | cut -d'"' -f4)
  echo "  Создан сервисный аккаунт с ID: $TRIGGER_SA_ID"
else
  echo "  Найден существующий аккаунт с ID: $TRIGGER_SA_ID"
fi

echo "Выдача прав functions.editor аккаунту $TRIGGER_SA_NAME..."
yc resource-manager folder add-access-binding "$FOLDER_ID" \
  --role functions.editor \
  --service-account-id "$TRIGGER_SA_ID" >/dev/null 2>&1

echo "Выдача прав iam.serviceAccounts.user аккаунту $TRIGGER_SA_NAME..."
yc resource-manager folder add-access-binding "$FOLDER_ID" \
  --role iam.serviceAccounts.user \
  --service-account-id "$TRIGGER_SA_ID" >/dev/null 2>&1

echo "Выдача прав functions.functionInvoker аккаунту s3-writer..."
yc resource-manager folder add-access-binding "$FOLDER_ID" \
  --role functions.functionInvoker \
  --service-account-id "$SA_S3_ID" >/dev/null 2>&1

echo ""
echo "== Таймер-триггеры (ежедневно в 04:00 UTC) =="
for FN in gen-pos-sales gen-loyalty gen-online-orders; do
  echo "Создание триггера для $FN..."
  # Убрано перенаправление в /dev/null, чтобы видеть ошибки, если они возникнут
  yc serverless trigger create timer \
    --name "$FN-timer" \
    --cron-expression "0 4 * * ? *" \
    --invoke-function-name "$FN" \
    --invoke-function-service-account-id "$SA_S3_ID" \
    --impersonate-service-account-id "$TRIGGER_SA_ID" || echo "  [!] Не удалось создать триггер для $FN (см. ошибку выше)"
done

echo ""
echo "Готово! Проверьте триггеры командой: yc serverless trigger list"
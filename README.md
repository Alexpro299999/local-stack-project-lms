# LocalStack Project LMS

ETL-пайплайн для обработки данных велопроката Хельсинки с эмуляцией AWS сервисов. Использует Airflow, Spark, Lambda и DynamoDB.

## Предварительные требования

Перед началом работы убедитесь, что установлены:
1.  **Docker Desktop** (включая Docker Compose).
2.  **Python 3.8+**.
3.  **Java JDK 17** (обязательно для работы PySpark локально и в контейнерах).

## Установка

1.  Клонируйте репозиторий и перейдите в папку проекта.
2.  Создайте виртуальное окружение Python (опционально, но рекомендуется).
3.  Установите необходимые библиотеки:
    ```bash
    pip install -r requirements.txt
    ```

## Запуск инфраструктуры

Выполняйте команды строго в указанном порядке.

1.  **Поднятие контейнеров**
    Запустите сервисы LocalStack и Airflow. Флаг `--build` обязателен при первом запуске для сборки образа с Java.
    ```bash
    docker-compose up -d --build
    ```
    Дождитесь, пока контейнеры перейдут в статус `Running`. Airflow будет доступен по адресу: http://localhost:8081 (логин/пароль: `airflow`/`airflow`).

2.  **Настройка ресурсов AWS**
    Скрипт создаст S3 бакет, SQS очередь, SNS топик и таблицу DynamoDB в LocalStack.
    ```bash
    python scripts/setup_infrastructure.py
    ```

3.  **Деплой Lambda функции**
    Скрипт соберет Docker-образ для лямбды, загрузит его и настроит триггер на SQS.
    ```bash
    python scripts/deploy_lambda.py
    ```

## Процесс обработки данных

1.  **Подготовка исходных данных**
    * Скачайте датасет велопроката.
    * Положите исходный `.csv` файл в папку `data/raw/` (создайте папку, если ее нет).
    * Запустите скрипт нарезки данных по месяцам:
        ```bash
        python scripts/split_data_by_month.py
        ```
    * Результат появится в `data/processed/`.

2.  **Загрузка данных в S3**
    * Откройте Airflow (http://localhost:8081).
    * Включите и запустите DAG `01_bulk_upload_to_s3_v6`.
    * Он загрузит файлы в бакет `my-helsinki-bikes-bucket/raw/`.

3.  **Расчет метрик (Spark)**
    * В Airflow запустите DAG `02_process_files_metrics_spark_v3`.
    * Он обработает файлы через Spark, рассчитает метрики и сохранит их в S3 с префиксом `metrics/`.

4.  **Сохранение в БД (Lambda)**
    * Lambda функция сработает автоматически при появлении новых файлов метрик в S3.
    * Она распарсит данные и запишет их в таблицу DynamoDB.
    * Проверить логи работы можно командой:
        ```bash
        python scripts/check_logs.py
        ```

## Экспорт результатов

Для визуализации данных в Tableau выгрузите содержимое DynamoDB в CSV:
```bash
python scripts/export_metrics.py
```
Файл будет сохранен как data/final_metrics_for_tableau.csv.

Локальная отладка (Опционально)
Если нужно проверить логику Spark и записи в базу без запуска Airflow:

```Bash
python scripts/run_logic_locally.py
```

#### Потом экспортируем результаты в фин файл для Tableau

```bash
python scripts/export_metrics.py
```
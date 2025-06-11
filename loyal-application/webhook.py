from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
import logging
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import base64
import socket
import sys
import psycopg2
from psycopg2 import sql
import json
import threading
import time

app = Flask(__name__)
auth = HTTPBasicAuth()

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логирование
logging.basicConfig(
    filename=f'webhook_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Пользователи для Basic Auth вебхука
WEBHOOK_USERNAME = os.getenv("WEBHOOK_USERNAME", "webhook_user")
users = {
    WEBHOOK_USERNAME: os.getenv("WEBHOOK_PASSWORD", "secure_password_123")
}

# Конфиг для listmonk API
LISTMONK_API_URL = os.getenv("LISTMONK_API_URL", "http://localhost:9000/api/subscribers")
LISTMONK_USERNAME = os.getenv("LISTMONK_USERNAME", "api_username")
LISTMONK_API_KEY = os.getenv("LISTMONK_API_KEY", "your-listmonk-api-key")

# Конфиг для marketingcrm API
MCRM_API_URL_USER = os.getenv("MCRM_API_URL_USER", "https://localhost")
MCRM_API_KEY = os.getenv("MCRM_API_KEY", "your-mcrm-api-key")

# Конфиг для PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "webhook_db")
DB_USER = os.getenv("DB_USER", "webhook_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_postgres_password")

# Интервал повторных попыток (в секундах)
RETRY_INTERVAL = 300  # 5 минут
MAX_RETRIES = 3  # Максимум 3 попытки

# Логируем загрузку конфигурации
logger.info(f"Инициализация приложения: WEBHOOK_USERNAME={WEBHOOK_USERNAME}, LISTMONK_API_URL={LISTMONK_API_URL}, MCRM_API_URL_USER={MCRM_API_URL_USER}, DB_HOST={DB_HOST}, DB_NAME={DB_NAME}")

# Подключение к PostgreSQL и создание таблиц
def init_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        # Создание таблиц для ошибок, очереди повторных попыток и финальной таблицы
        tables = [
            """
            CREATE TABLE IF NOT EXISTS requests_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                method VARCHAR(10),
                path VARCHAR(255),
                headers TEXT,
                remote_addr VARCHAR(45),
                serial VARCHAR(255),
                event VARCHAR(100),
                error_message TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS serial_processing_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                original_serial VARCHAR(255),
                cleaned_serial VARCHAR(255),
                error_message TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mcrm_requests_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                url VARCHAR(255),
                number VARCHAR(255),
                status_code INTEGER,
                response TEXT,
                error_message TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS listmonk_requests_log (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                url VARCHAR(255),
                payload TEXT,
                status_code INTEGER,
                response TEXT,
                error_message TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                uuid VARCHAR(36),
                email VARCHAR(255),
                phone VARCHAR(20),
                status BOOLEAN DEFAULT FALSE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS retry_queue (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                serial VARCHAR(255),
                event VARCHAR(100),
                payload TEXT,
                retry_count INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                error_message TEXT
            )
            """
        ]
        
        for table_query in tables:
            cursor.execute(table_query)
        
        conn.commit()
        logger.info("Все таблицы базы данных успешно инициализированы")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {e}")
        sys.exit(1)

# Проверка сетевых интерфейсов и порта
def log_network_interfaces(port=5001):
    try:
        hostname = socket.gethostname()
        ip_addresses = socket.gethostbyname_ex(hostname)[2]
        logger.info(f"Сервер запущен на хосте: hostname={hostname}, IP-адреса={ip_addresses}")
        
        # Проверяем, занят ли порт
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('0.0.0.0', port))
        if result == 0:
            logger.error(f"Порт {port} уже занят")
            raise socket.error(f"Порт {port} уже занят")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('0.0.0.0', port))
        logger.debug(f"Порт {port} успешно привязан")
        sock.close()
    except socket.error as e:
        logger.error(f"Ошибка привязки к порту {port}: {e}")
        raise

# Функция для записи ошибок в базу данных
def log_error_to_db(table, data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        if table == "requests_log":
            query = sql.SQL("""
                INSERT INTO requests_log (method, path, headers, remote_addr, serial, event, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(query, (
                data['method'],
                data['path'],
                data['headers'],
                data['remote_addr'],
                data['serial'],
                data['event'],
                data['error_message']
            ))
        elif table == "serial_processing_log":
            query = sql.SQL("""
                INSERT INTO serial_processing_log (original_serial, cleaned_serial, error_message)
                VALUES (%s, %s, %s)
            """)
            cursor.execute(query, (
                data['original_serial'],
                data['cleaned_serial'],
                data['error_message']
            ))
        elif table == "mcrm_requests_log":
            query = sql.SQL("""
                INSERT INTO mcrm_requests_log (url, number, status_code, response, error_message)
                VALUES (%s, %s, %s, %s, %s)
            """)
            cursor.execute(query, (
                data['url'],
                data['number'],
                data['status_code'],
                data['response'],
                data['error_message']
            ))
        elif table == "listmonk_requests_log":
            query = sql.SQL("""
                INSERT INTO listmonk_requests_log (url, payload, status_code, response, error_message)
                VALUES (%s, %s, %s, %s, %s)
            """)
            cursor.execute(query, (
                data['url'],
                data['payload'],
                data['status_code'],
                data['response'],
                data['error_message']
            ))
        
        conn.commit()
        logger.debug(f"Ошибка записана в таблицу {table}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка записи в таблицу {table}: {e}")

# Функция для записи в таблицу subscribers
def log_subscriber_to_db(uuid, email, phone):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        query = sql.SQL("""
            INSERT INTO subscribers (uuid, email, phone, status)
            VALUES (%s, %s, %s, %s)
        """)
        cursor.execute(query, (uuid, email, phone, False))
        
        conn.commit()
        logger.debug(f"Запись добавлена в таблицу subscribers: uuid={uuid}, email={email}, phone={phone}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка записи в таблицу subscribers: {e}")

# Функция для записи в очередь повторных попыток
def add_to_retry_queue(serial, event, payload, error_message):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        query = sql.SQL("""
            INSERT INTO retry_queue (serial, event, payload, error_message)
            VALUES (%s, %s, %s, %s)
        """)
        cursor.execute(query, (serial, event, json.dumps(payload) if payload else None, error_message))
        
        conn.commit()
        logger.debug(f"Добавлена запись в retry_queue: serial={serial}, event={event}")
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка записи в retry_queue: {e}")

# Функция для обработки записей из retry_queue
def process_retry_queue():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = conn.cursor()
            
            # Извлекаем записи, где last_attempt старше RETRY_INTERVAL или NULL, и retry_count < MAX_RETRIES
            query = sql.SQL("""
                SELECT id, serial, event, payload, retry_count
                FROM retry_queue
                WHERE (last_attempt IS NULL OR last_attempt < %s) AND retry_count < %s
            """)
            cursor.execute(query, (datetime.now() - timedelta(seconds=RETRY_INTERVAL), MAX_RETRIES))
            rows = cursor.fetchall()
            
            for row in rows:
                retry_id, serial, event, payload, retry_count = row
                logger.info(f"Повторная обработка записи из retry_queue: id={retry_id}, serial={serial}, retry_count={retry_count}")
                
                try:
                    # Увеличиваем retry_count и обновляем last_attempt
                    cursor.execute(
                        sql.SQL("UPDATE retry_queue SET retry_count = %s, last_attempt = %s WHERE id = %s"),
                        (retry_count + 1, datetime.now(), retry_id)
                    )
                    conn.commit()
                    
                    if event == "cardcreate":
                        cleaned_serial = serial.split('-')[0] if '-' in serial else serial
                        if not cleaned_serial:
                            logger.error(f"Пустой serial после очистки в retry_queue: id={retry_id}")
                            cursor.execute(
                                sql.SQL("UPDATE retry_queue SET error_message = %s WHERE id = %s"),
                                ("Пустой serial после очистки", retry_id)
                            )
                            conn.commit()
                            continue
                        
                        # Запрос к MCRM
                        logger.info(f"Повторный запрос к MCRM: number={cleaned_serial}")
                        mcrm_response = requests.get(
                            MCRM_API_URL_USER,
                            params={"number": cleaned_serial, "api_key": MCRM_API_KEY}
                        )
                        
                        if mcrm_response.status_code != 200:
                            logger.error(f"Ошибка повторного запроса MCRM: id={retry_id}, status_code={mcrm_response.status_code}")
                            log_error_to_db("mcrm_requests_log", {
                                "url": MCRM_API_URL_USER,
                                "number": cleaned_serial,
                                "status_code": mcrm_response.status_code,
                                "response": mcrm_response.text,
                                "error_message": f"MCRM API error: {mcrm_response.status_code}"
                            })
                            cursor.execute(
                                sql.SQL("UPDATE retry_queue SET error_message = %s WHERE id = %s"),
                                (f"MCRM API error: {mcrm_response.status_code}", retry_id)
                            )
                            conn.commit()
                            continue
                        
                        mcrm_data = mcrm_response.json()
                        if not mcrm_data.get('email'):
                            logger.error(f"Email не найден в MCRM: id={retry_id}")
                            log_error_to_db("mcrm_requests_log", {
                                "url": MCRM_API_URL_USER,
                                "number": cleaned_serial,
                                "status_code": mcrm_response.status_code,
                                "response": mcrm_response.text,
                                "error_message": "Email не найден"
                            })
                            cursor.execute(
                                sql.SQL("UPDATE retry_queue SET error_message = %s WHERE id = %s"),
                                ("Email не найден", retry_id)
                            )
                            conn.commit()
                            continue
                        
                        email = mcrm_data['email']
                        phone = mcrm_data.get('phone', '')
                        
                        # Используем сохранённый payload или формируем новый
                        payload_dict = json.loads(payload) if payload else {
                            "email": email,
                            "name": f"{mcrm_data.get('first_name', '')} {mcrm_data.get('last_name', '')}".strip() or 'Unknown',
                            "status": "enabled",
                            "lists": [1],
                            "attribs": {
                                "phone": phone,
                                "birth_date": mcrm_data.get('birth_date', ''),
                                "gender": mcrm_data.get('gender', ''),
                                "card_number": mcrm_data.get('card_number', ''),
                                "balance": mcrm_data.get('balance', 0),
                                "check_count": mcrm_data.get('check_count', 0),
                                "average_check": mcrm_data.get('average_check', 0),
                                "register_date": mcrm_data.get('register_date', ''),
                                "last_visit_date": mcrm_data.get('last_visit_date', ''),
                                "resto_id": mcrm_data.get('resto_id', 0),
                                "osmi_setup": mcrm_data.get('osmi_setup', False),
                                "segments": [segment['name'] for segment in mcrm_data.get('segments', [])]
                            }
                        }
                        
                        # Запрос к listmonk
                        auth_str = f"{LISTMONK_USERNAME}:{LISTMONK_API_KEY}"
                        auth_header = {"Authorization": f"Basic {base64.b64encode(auth_str.encode()).decode()}"}
                        logger.info(f"Повторный запрос к listmonk: id={retry_id}")
                        listmonk_response = requests.post(
                            LISTMONK_API_URL,
                            json=payload_dict,
                            headers={
                                **auth_header,
                                "Content-Type": "application/json"
                            }
                        )
                        
                        if listmonk_response.status_code not in [200, 201]:
                            logger.error(f"Ошибка повторного запроса listmonk: id={retry_id}, status_code={listmonk_response.status_code}")
                            log_error_to_db("listmonk_requests_log", {
                                "url": LISTMONK_API_URL,
                                "payload": json.dumps(payload_dict),
                                "status_code": listmonk_response.status_code,
                                "response": listmonk_response.text,
                                "error_message": f"listmonk API error: {listmonk_response.status_code}"
                            })
                            cursor.execute(
                                sql.SQL("UPDATE retry_queue SET error_message = %s WHERE id = %s"),
                                (f"listmonk API error: {listmonk_response.status_code}", retry_id)
                            )
                            conn.commit()
                            continue
                        
                        # Успех, записываем в subscribers и удаляем из retry_queue
                        listmonk_data = listmonk_response.json()
                        uuid = listmonk_data.get('data', {}).get('id') or listmonk_data.get('uuid', 'unknown')
                        log_subscriber_to_db(uuid, email, phone)
                        logger.info(f"Успешная повторная обработка: id={retry_id}, uuid={uuid}")
                        
                        cursor.execute(sql.SQL("DELETE FROM retry_queue WHERE id = %s"), (retry_id,))
                        conn.commit()
                        logger.debug(f"Запись удалена из retry_queue: id={retry_id}")
                
                except Exception as e:
                    logger.error(f"Ошибка повторной обработки: id={retry_id}, error={e}")
                    cursor.execute(
                        sql.SQL("UPDATE retry_queue SET error_message = %s WHERE id = %s"),
                        (str(e), retry_id)
                    )
                    conn.commit()
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка обработки retry_queue: {e}")
        
        time.sleep(RETRY_INTERVAL)

# Health check эндпоинт
@app.route('/health', methods=['GET'])
def health_check():
    logger.info("Получен запрос на /health")
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200

# Проверка логина и пароля для вебхука
@auth.verify_password
def verify_password(username, password):
    logger.info(f"Проверка авторизации: username={username}")
    if username in users and users[username] == password:
        logger.info(f"Авторизация успешна для username={username}")
        return username
    logger.warning(f"Неудачная авторизация для username={username}")
    return None

@app.route('/webhook', methods=['GET', 'POST'])
@auth.login_required(optional=True)
def webhook():
    request_data = {
        "method": request.method,
        "path": request.path,
        "headers": str(request.headers),
        "remote_addr": request.remote_addr,
        "serial": None,
        "event": None,
        "error_message": None
    }

    if request.method == 'GET':
        serial = request.args.get('serial')
        event = request.args.get('event')
        logger.info(f"GET запрос: serial={serial}, event={event}")
        if not serial or not event:
            logger.error(f"Отсутствует параметр serial или event: serial={serial}, event={event}")
            request_data.update({"serial": serial, "event": event, "error_message": "Отсутствует параметр serial или event"})
            log_error_to_db("requests_log", request_data)
            return jsonify({"error": "Отсутствует параметр serial или event"}), 400
        if event != "cardcreate":
            logger.error(f"Неподдерживаемое событие: event={event}")
            request_data.update({"serial": serial, "event": event, "error_message": f"Неподдерживаемое событие: {event}"})
            log_error_to_db("requests_log", request_data)
            return jsonify({"error": "Неподдерживаемое событие"}), 400
        request_data.update({"serial": serial, "event": event})
        data = {"serial": serial, "event": event}
    elif request.method == 'POST':
        serial = request.form.get('serial')
        event = request.form.get('event')
        logger.info(f"POST запрос: serial={serial}, event={event}")
        if not serial or not event:
            logger.error(f"Отсутствует параметр serial или event: serial={serial}, event={event}")
            request_data.update({"serial": serial, "event": event, "error_message": "Отсутствует параметр serial или event"})
            log_error_to_db("requests_log", request_data)
            return jsonify({"error": "Отсутствует параметр serial или event"}), 400
        if event != "cardcreate":
            logger.error(f"Неподдерживаемое событие: event={event}")
            request_data.update({"serial": serial, "event": event, "error_message": f"Неподдерживаемое событие: {event}"})
            log_error_to_db("requests_log", request_data)
            return jsonify({"error": "Неподдерживаемое событие"}), 400
        request_data.update({"serial": serial, "event": event})
        data = {"serial": serial, "event": event}

    user = auth.current_user() or 'anonymous'
    logger.info(f"Текущий пользователь: {user}")

    try:
        logger.info(f"Обработка события cardcreate: serial={data['serial']}")
        
        # Обрезаем serial до дефиса
        cleaned_serial = data['serial'].split('-')[0] if '-' in data['serial'] else data['serial']
        logger.info(f"Обработан serial: исходный={data['serial']}, очищенный={cleaned_serial}")
        if not cleaned_serial:
            logger.error(f"Ошибка обработки serial: пустой после очистки")
            log_error_to_db("serial_processing_log", {
                "original_serial": data['serial'],
                "cleaned_serial": cleaned_serial,
                "error_message": "Пустой serial после очистки"
            })
            add_to_retry_queue(data['serial'], data['event'], None, "Пустой serial после очистки")
            return jsonify({"error": "Ошибка обработки serial"}), 400

        # Запрос к marketingcrm API
        logger.info(f"Отправка запроса к MCRM API: URL={MCRM_API_URL_USER}, number={cleaned_serial}")
        try:
            mcrm_response = requests.get(
                MCRM_API_URL_USER,
                params={
                    "number": cleaned_serial,
                    "api_key": MCRM_API_KEY
                }
            )
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса к MCRM API: {e}")
            log_error_to_db("mcrm_requests_log", {
                "url": MCRM_API_URL_USER,
                "number": cleaned_serial,
                "status_code": None,
                "response": None,
                "error_message": f"MCRM API request error: {str(e)}"
            })
            add_to_retry_queue(data['serial'], data['event'], None, f"MCRM API request error: {str(e)}")
            return jsonify({"error": "Ошибка запроса к MCRM API"}), 500
        
        logger.info(f"Ответ от MCRM API: status_code={mcrm_response.status_code}")
        if mcrm_response.status_code != 200:
            logger.error(f"Ошибка запроса к MCRM API: status_code={mcrm_response.status_code}, response={mcrm_response.text}")
            log_error_to_db("mcrm_requests_log", {
                "url": MCRM_API_URL_USER,
                "number": cleaned_serial,
                "status_code": mcrm_response.status_code,
                "response": mcrm_response.text,
                "error_message": f"MCRM API error: {mcrm_response.status_code}"
            })
            add_to_retry_queue(data['serial'], data['event'], None, f"MCRM API error: {mcrm_response.status_code}")
            return jsonify({"error": "Ошибка запроса к MCRM API"}), 500

        mcrm_data = mcrm_response.json()
        logger.info(f"Получен ответ от MCRM API: status={mcrm_data.get('status')}, user_id={mcrm_data.get('user_id')}, email={mcrm_data.get('email')}")
        
        if not mcrm_data.get('email'):
            logger.error(f"Email не найден в ответе MCRM: status={mcrm_data.get('status')}")
            log_error_to_db("mcrm_requests_log", {
                "url": MCRM_API_URL_USER,
                "number": cleaned_serial,
                "status_code": mcrm_response.status_code,
                "response": mcrm_response.text,
                "error_message": "Email не найден"
            })
            add_to_retry_queue(data['serial'], data['event'], None, "Email не найден")
            return jsonify({"error": "Email не найден в ответе MCRM"}), 400

        email = mcrm_data['email']
        phone = mcrm_data.get('phone', '')
        logger.info(f"Извлечён email: {email}, phone: {phone}")
        
        # Собираем имя из first_name и last_name
        first_name = mcrm_data.get('first_name', '')
        last_name = mcrm_data.get('last_name', '')
        name = f"{first_name} {last_name}".strip()
        logger.info(f"Сформировано имя: first_name={first_name}, last_name={last_name}, name={name}")

        # Формируем attribs из ответа MCRM
        attribs = {
            "phone": phone,
            "birth_date": mcrm_data.get('birth_date', ''),
            "gender": mcrm_data.get('gender', ''),
            "card_number": mcrm_data.get('card_number', ''),
            "balance": mcrm_data.get('balance', 0),
            "check_count": mcrm_data.get('check_count', 0),
            "average_check": mcrm_data.get('average_check', 0),
            "register_date": mcrm_data.get('register_date', ''),
            "last_visit_date": mcrm_data.get('last訪_date', ''),
            "resto_id": mcrm_data.get('resto_id', 0),
            "osmi_setup": mcrm_data.get('osmi_setup', False)
        }
        if mcrm_data.get('segments'):
            attribs['segments'] = [segment['name'] for segment in mcrm_data.get('segments', [])]
        logger.info(f"Сформированы attribs для listmonk: {attribs}")

        # Формируем payload для listmonk
        listmonk_payload = {
            "email": email,
            "name": name,
            "status": "enabled",
            "lists": [1],
            "attribs": attribs
        }
        logger.info(f"Подготовлен payload для listmonk: {listmonk_payload}")

        # Формируем Basic Auth заголовок для listmonk
        auth_str = f"{LISTMONK_USERNAME}:{LISTMONK_API_KEY}"
        auth_header = {"Authorization": f"Basic {base64.b64encode(auth_str.encode()).decode()}"}
        logger.info(f"Подготовлен заголовок авторизации для listmonk")

        # Отправляем в listmonk
        logger.info(f"Отправка запроса к listmonk API: URL={LISTMONK_API_URL}")
        try:
            listmonk_response = requests.post(
                LISTMONK_API_URL,
                json=listmonk_payload,
                headers={
                    **auth_header,
                    "Content-Type": "application/json"
                }
            )
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса к listmonk API: {e}")
            log_error_to_db("listmonk_requests_log", {
                "url": LISTMONK_API_URL,
                "payload": json.dumps(listmonk_payload),
                "status_code": None,
                "response": None,
                "error_message": f"listmonk API request error: {str(e)}"
            })
            add_to_retry_queue(data['serial'], data['event'], listmonk_payload, f"listmonk API request error: {str(e)}")
            return jsonify({"error": "Ошибка запроса к listmonk API"}), 500

        logger.info(f"Ответ от listmonk API: status_code={listmonk_response.status_code}")
        if listmonk_response.status_code not in [200, 201]:
            logger.error(f"Ошибка запроса к listmonk API: status_code={listmonk_response.status_code}, response={listmonk_response.text}")
            log_error_to_db("listmonk_requests_log", {
                "url": LISTMONK_API_URL,
                "payload": json.dumps(listmonk_payload),
                "status_code": listmonk_response.status_code,
                "response": listmonk_response.text,
                "error_message": f"listmonk API error: {listmonk_response.status_code}"
            })
            add_to_retry_queue(data['serial'], data['event'], listmonk_payload, f"listmonk API error: {listmonk_response.status_code}")
            return jsonify({"error": "Ошибка запроса к listmonk API"}), 500

        # Успех, записываем в subscribers
        listmonk_data = listmonk_response.json()
        uuid = listmonk_data.get('data', {}).get('id') or listmonk_data.get('uuid', 'unknown')
        logger.info(f"Извлечён UUID из listmonk: {uuid}")
        log_subscriber_to_db(uuid, email, phone)
        
        logger.info(f"Событие cardcreate успешно обработано: serial={data['serial']}, email={email}, user={user}, uuid={uuid}")
    except Exception as e:
        logger.error(f"Общая ошибка обработки: {e}, data: {data}")
        add_to_retry_queue(data['serial'], data['event'], None, f"Общая ошибка: {str(e)}")
        return jsonify({"error": "Общая ошибка обработки"}), 500

    logger.info(f"Запрос успешно обработан, возвращён ответ: status=success")
    return jsonify({"status": "success"}), 200

if __name__ == "__main__":
    try:
        logger.info("Инициализация базы данных PostgreSQL")
        init_db()
        logger.info("Запуск Flask приложения на host='0.0.0.0', port=5001")
        log_network_interfaces(port=5001)
        
        # Запускаем фоновую задачу для обработки retry_queue
        retry_thread = threading.Thread(target=process_retry_queue, daemon=True)
        retry_thread.start()
        logger.info("Фоновая задача для retry_queue запущена")
        
        app.run(debug=True, host='0.0.0.0', port=5001, use_reloader=False)
    except Exception as e:
        logger.error(f"Ошибка при запуске приложения: {e}")
        sys.exit(1)
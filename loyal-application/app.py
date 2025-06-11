import os
import requests
import psycopg2
import schedule
import time
from dotenv import load_dotenv
import logging
import base64
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка переменных из .env
load_dotenv()

# Проверка наличия всех обязательных переменных окружения
REQUIRED_ENV_VARS = [
    'LISTMONK_API_URL',
    'LISTMONK_API_USER',
    'LISTMONK_API_TOKEN',
    'MCRM_API_URL_BONUS',
    'MCRM_API_TOKEN',
    'BONUS_SUM',
    'LIST_ID',
    'DB_HOST',
    'DB_PORT',
    'DB_NAME',
    'DB_USER',
    'DB_PASSWORD'
]

missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing_vars:
    logger.error(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
    exit(1)

# Конфигурация
LISTMONK_API_URL = os.getenv('LISTMONK_API_URL')
LISTMONK_API_USER = os.getenv('LISTMONK_API_USER')
LISTMONK_API_TOKEN = os.getenv('LISTMONK_API_TOKEN')
MCRM_API_URL_BONUS = os.getenv('MCRM_API_URL_BONUS')
MCRM_API_TOKEN = os.getenv('MCRM_API_TOKEN')
BONUS_SUM = float(os.getenv('BONUS_SUM'))
LIST_ID = int(os.getenv('LIST_ID'))
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Логирование используемых параметров
logger.info(f"Используемый MCRM_API_URL_BONUS: {MCRM_API_URL_BONUS}")

# Инициализация базы данных
def init_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscribers (
                uid TEXT PRIMARY KEY,
                phone TEXT NOT NULL,
                bonus_added BOOLEAN DEFAULT FALSE
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("База данных инициализирована")
    except psycopg2.Error as e:
        logger.error(f"Ошибка инициализации базы данных: {e}")

# Получение подписчиков из listmonk
def get_subscribers():
    # Формирование заголовка Basic Auth
    auth_string = f"{LISTMONK_API_USER}:{LISTMONK_API_TOKEN}"
    auth_encoded = base64.b64encode(auth_string.encode()).decode()
    headers = {
        'Authorization': f'Basic {auth_encoded}',
        'Content-Type': 'application/json'
    }
    params = {
        'list_id': LIST_ID,
        'status': 'enabled',
        'page': 1
    }
    all_subscribers = []
    
    try:
        while True:
            response = requests.get(LISTMONK_API_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Логирование структуры ответа
            logger.debug(f"Ответ API listmonk (страница {params['page']}): {json.dumps(data, indent=2)}")
            
            # Проверка структуры ответа
            if not isinstance(data, dict) or 'data' not in data:
                logger.error("Некорректная структура ответа: поле 'data' отсутствует или ответ не является JSON")
                return all_subscribers
            
            subscribers_data = data.get('data', {})
            if not isinstance(subscribers_data, dict):
                logger.error(f"Поле 'data' не является словарем: {type(subscribers_data)}")
                return all_subscribers
            
            subscribers = subscribers_data.get('results', [])
            if not isinstance(subscribers, list):
                logger.error(f"Поле 'data.results' не является списком: {type(subscribers)}")
                return all_subscribers
                
            all_subscribers.extend(subscribers)
            
            # Проверка пагинации
            if 'next' not in subscribers_data or not subscribers_data['next']:
                break
            params['page'] += 1
            logger.info(f"Обработка следующей страницы: {params['page']}")
            
        return all_subscribers
    except ValueError as e:
        logger.error(f"Ошибка разбора JSON ответа: {e}")
        return all_subscribers
    except Exception as e:
        logger.error(f"Ошибка получения подписчиков: {e}")
        return all_subscribers

# Сохранение подписчика в базу данных
def save_subscriber(uid, phone):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO subscribers (uid, phone, bonus_added)
            VALUES (%s, %s, %s)
            ON CONFLICT (uid) DO NOTHING
        ''', (uid, phone, False))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Подписчик {uid} сохранен в базе данных")
    except ValueError as e:
        logger.error(f"Ошибка сохранения подписчика {uid}: {e}")
    except Exception as e:
        logger.error(f"Ошибка сохранения {uid}: {e}")

# Проверка статуса начисления бонусов
def check_bonus_status(uid):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = conn.cursor()
        cursor.execute('SELECT bonus_added FROM subscribers WHERE uid = %s', (uid,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else False
    except ValueError as e:
        logger.error(f"Ошибка проверки статуса бонусов для {uid}: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки статуса бонусов для {uid}: {e}")
        return False

# Обновление статуса начисления бонусов
def update_bonus_status(uid):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute('UPDATE subscribers SET bonus_added = %s WHERE uid = %s', (True, uid))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Статус бонусов обновлен для {uid}")
    except ValueError as e:
        logger.error(f"Ошибка обновления статуса бонусов для {uid}: {e}")
    except Exception as e:
        logger.error(f"Ошибка обновления статуса бонусов для {uid}: {e}")

# Начисление бонусов через MCRM API
def add_bonus(phone, uid):
    headers = {
        'x-api-key': MCRM_API_TOKEN,
        'Content-Type': 'application/json'
    }
    payload = {
        'number': phone,
        'sum': BONUS_SUM,
        'comment': 'MAIL SUBSCRIBE'
    }
    
    try:
        logger.debug(f"Отправка запроса к MCRM API: URL={MCRM_API_URL_BONUS}, headers={{'x-api-key': '***', 'Content-Type': 'application/json'}}, payload={payload}")
        response = requests.post(MCRM_API_URL_BONUS, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Бонусы начислены для телефона {phone}, UID: {uid}")
        return True
    except ValueError as e:
        logger.error(f"Ошибка начисления бонусов для телефона {phone}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Код ответа: {e.response.status_code}, Текст ответа: {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"Ошибка начисления бонусов для телефона {phone}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Код ответа: {e.response.status_code}, Текст ответа: {e.response.text}")
        return False

# Основная функция обработки
def process_subscribers():
    logger.info("Начало обработки подписчиков")
    
    # Получение подписчиков
    subscribers = get_subscribers()
    if not subscribers:
        logger.warning("Список подписчиков пуст или не удалось получить данные")
        return
    
    # Обработка каждого подписчика
    for subscriber in subscribers:
        if not isinstance(subscriber, dict):
            logger.error(f"Подписчик не является словарем: {subscriber}")
            continue
            
        uid = subscriber.get('uuid')
        status = subscriber.get('status')
        phone = subscriber.get('attribs', {}).get('phone')
        lists = subscriber.get('lists', [])
        
        # Проверка статуса подписки для указанного list_id
        is_confirmed = False
        for lst in lists:
            if lst.get('id') == LIST_ID and lst.get('subscription_status') == 'confirmed':
                is_confirmed = True
                break
        
        if not uid or not phone or status != 'enabled' or not is_confirmed:
            logger.debug(f"Пропущен подписчик: UID={uid}, phone={phone}, status={status}, confirmed={is_confirmed}")
            continue
            
        # Сохранение подписчика в БД
        save_subscriber(uid, phone)
        
        # Проверка статуса бонусов
        if check_bonus_status(uid):
            logger.debug(f"Бонусы уже начислены для UID={uid}")
            continue
            
        # Начисление бонусов
        if add_bonus(phone, uid):
            update_bonus_status(uid)
        else:
            logger.warning(f"Не удалось начислить бонусы для UID={uid}")

# Точка входа
def main():
    init_db()
    process_subscribers()  # Выполнить сразу при запуске
    schedule.every(1).hours.do(process_subscribers)
    
    logger.info("Скрипт запущен, ожидание выполнения по расписанию")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)  # Проверка каждую минуту
        except Exception as e:
            logger.error(f"Ошибка ввода: {e}")

if __name__ == "__main__":
    main()
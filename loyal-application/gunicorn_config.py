import multiprocessing
import os

# Количество воркеров (2 * CPU + 1)
workers = multiprocessing.cpu_count() * 2 + 1
threads = 2

# Привязка к порту и хосту
bind = "0.0.0.0:5002"

# Логирование
accesslog = "/app/logs/gunicorn_access.log"
errorlog = "/app/logs/gunicorn_error.log"
loglevel = "info"

# Таймауты
timeout = 60
keepalive = 5

# Отключение перезагрузки в продакшене
reload = False

# Название приложения для логов
proc_name = "stable"

# Буферизация вывода для реального времени
capture_output = True
enable_stdio_inheritance = True
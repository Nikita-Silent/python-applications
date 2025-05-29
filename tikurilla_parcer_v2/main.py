import asyncio
import aiohttp
from pathlib import Path
from bs4 import BeautifulSoup
import logging

# Настройка логирования
logging.basicConfig(
    filename='errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Базовый URL для страниц с цветами
base_page_url = "https://tikkurila.com/pro/collection/symphony-color-system?keyword="

# Папка для сохранения изображений
output_dir = Path("color_images")
output_dir.mkdir(exist_ok=True)

# Словарь для хранения путей к сохраненным изображениям
new_color_dict = {}

# Заголовки для имитации браузера
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

async def fetch_page(session, code):
    page_url = f"{base_page_url}{code}"
    file_name = f"{code}.png"
    file_path = output_dir / file_name

    # Проверяем, существует ли файл
    if file_path.exists():
        print(f"Изображение для {code} уже существует: {file_path}")
        return code, str(file_path)

    try:
        async with session.get(page_url, headers=headers) as response:
            if response.status != 200:
                print(f"Ошибка загрузки страницы для {code}: статус {response.status}")
                logging.error(f"Ошибка загрузки страницы для {code}: статус {response.status}")
                return code, "Ошибка загрузки"
            
            # Парсим HTML
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            img_tag = soup.find('img', class_='image-style-scale-crop-large-480-480')
            
            if not img_tag or 'src' not in img_tag.attrs:
                print(f"Изображение не найдено на странице для {code}")
                logging.error(f"Изображение не найдено на странице для {code}")
                return code, "Ошибка загрузки"
            
            # Получаем URL изображения
            image_url = img_tag['src']
            if image_url.startswith('/'):
                image_url = f"https://tikkurila.com{image_url}"
            
            # Загружаем изображение
            async with session.get(image_url, headers=headers) as img_response:
                if img_response.status == 200:
                    with open(file_path, 'wb') as f:
                        f.write(await img_response.read())
                    print(f"Изображение для {code} сохранено: {file_path}")
                    return code, str(file_path)
                else:
                    print(f"Ошибка загрузки изображения для {code}: статус {img_response.status}")
                    logging.error(f"Ошибка загрузки изображения для {code}: статус {img_response.status}")
                    return code, "Ошибка загрузки"
                    
    except Exception as e:
        print(f"Ошибка при обработке {code}: {e}")
        logging.error(f"Ошибка при обработке {code}: {e}")
        return code, "Ошибка загрузки"

async def main():
    # Генерация кодов
    letters = ['F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'S', 'V', 'X', 'Y']
    codes = [f"{letter}{number}" for letter in letters for number in range(300, 503)]
    
    async with aiohttp.ClientSession() as session:
        # Создаем задачи для каждого кода
        tasks = [fetch_page(session, code) for code in codes]
        # Ограничиваем количество одновременных запросов
        semaphore = asyncio.Semaphore(10)  # Максимум 10 одновременных запросов
        async def sem_task(task):
            async with semaphore:
                return await task
        # Выполняем задачи
        results = await asyncio.gather(*[sem_task(task) for task in tasks], return_exceptions=True)
        
        # Заполняем финальный словарь
        success_count = 0
        error_count = 0
        for code, path in results:
            if not isinstance(path, Exception) and path != "Ошибка загрузки":
                new_color_dict[code] = path
                success_count += 1
            else:
                error_count += 1

        print(f"\nОбработка завершена: успешно загружено {success_count} изображений, ошибок: {error_count}")

# Запускаем асинхронный цикл
if __name__ == "__main__":
    asyncio.run(main())

    # Выводим финальный словарь
    print("\nФинальный словарь с путями к изображениям:")
    for code, path in new_color_dict.items():
        print(f"{code}: {path}")
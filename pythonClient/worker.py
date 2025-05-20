import sys
import io
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask, request, send_file
from PIL import Image, ImageFilter
import numpy as np

app = Flask(__name__)

# --- Настройка логгера для приложения Flask ---
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = RotatingFileHandler('worker.log', maxBytes=5*1024*1024, backupCount=3)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# Удаляем все существующие обработчики (включая вывод в консоль)
app.logger.handlers.clear()
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.DEBUG)

# --- Отключаем логгер werkzeug (HTTP сервера Flask) ---
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.handlers.clear()
werkzeug_logger.propagate = False  # не передавать дальше события в root logger

# Если нужно — можно добавить отдельный файловый обработчик для werkzeug, например:
# werkzeug_logger.addHandler(file_handler)
# werkzeug_logger.setLevel(logging.WARNING)  # или ERROR

def process_image_bytes(image_bytes: bytes) -> io.BytesIO:
    app.logger.debug("Начата обработка изображения")

    image = Image.open(io.BytesIO(image_bytes))
    app.logger.debug(f"Исходный формат изображения: {image.mode}, размер: {image.size}")

    gray_image = image.convert('L')
    app.logger.debug("Конвертация в оттенки серого завершена")

    blurred_image = gray_image.filter(ImageFilter.GaussianBlur(radius=2))
    app.logger.debug("Применено размытие Гаусса")

    img_array = np.array(blurred_image)
    threshold = 100
    binary_array = (img_array > threshold) * 255
    app.logger.debug("Пороговое бинаризирование завершено")

    result_image = Image.fromarray(binary_array.astype(np.uint8))

    output_buffer = io.BytesIO()
    result_image.save(output_buffer, format='PNG')
    output_buffer.seek(0)

    app.logger.debug("Обработка изображения завершена, возвращаем результат")

    return output_buffer


@app.route('/process', methods=['POST'])
def process_image():
    app.logger.info("Получен запрос на /process")

    if 'image' not in request.files:
        app.logger.error("В запросе отсутствует файл с ключом 'image'")
        return "No image file uploaded", 400

    image_file = request.files['image']
    image_bytes = image_file.read()

    try:
        result_buffer = process_image_bytes(image_bytes)
    except Exception as e:
        app.logger.exception(f"Ошибка при обработке изображения: {e}")
        return "Error processing image", 500

    app.logger.info("Обработка завершена успешно, отправляем результат")

    return send_file(result_buffer, mimetype='image/png')


if __name__ == '__main__':
    port = 5000
    app.logger.info(f"Запуск сервера на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

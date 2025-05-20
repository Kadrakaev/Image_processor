import sys
from flask import Flask, request, send_file
from PIL import Image, ImageFilter
import numpy as np
import io

app = Flask(__name__)


def process_image_bytes(image_bytes: bytes) -> io.BytesIO:
    """
    Принимает изображение в виде байтов,
    преобразует в PIL Image,
    применяет обработку (градации серого, блюр, бинаризация),
    возвращает результат в байтовом буфере PNG.
    """
    # Загружаем изображение из байтов
    image = Image.open(io.BytesIO(image_bytes))

    # Переводим изображение в оттенки серого
    gray_image = image.convert('L')

    # Применяем размытие по Гауссу с радиусом 2
    blurred_image = gray_image.filter(ImageFilter.GaussianBlur(radius=2))

    # Конвертируем изображение в numpy массив для бинаризации
    img_array = np.array(blurred_image)

    # Задаём порог для бинаризации
    threshold = 100

    # Создаём бинарный массив: пиксели > threshold будут 255, иначе 0
    binary_array = (img_array > threshold) * 255

    # Конвертируем обратно в изображение PIL
    result_image = Image.fromarray(binary_array.astype(np.uint8))

    # Создаём буфер для сохранения результата в формате PNG
    output_buffer = io.BytesIO()
    result_image.save(output_buffer, format='PNG')
    output_buffer.seek(0)  # Возврат к началу буфера

    return output_buffer


@app.route('/process', methods=['POST'])
def process_image():
    """
    Эндпоинт для обработки изображения.
    Ожидает raw байты изображения в теле POST-запроса.
    Возвращает обработанное изображение в формате PNG.
    """
    image_bytes = request.data  # читаем тело запроса как raw байты

    if not image_bytes:
        # Если тело пустое — возвращаем ошибку 400
        return "No image data", 400

    # Обрабатываем изображение
    result_buffer = process_image_bytes(image_bytes)

    # Отправляем обратно результат с заголовком mimetype
    return send_file(result_buffer, mimetype='image/png')


if __name__ == '__main__':
    # Получаем порт из аргументов запуска, по умолчанию 5000
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    # Запускаем Flask на всех интерфейсах
    app.run(host='0.0.0.0', port=port)

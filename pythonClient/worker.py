import sys
from flask import Flask, request, send_file
from PIL import Image, ImageFilter
import numpy as np
import io

app = Flask(__name__)

def process_image_bytes(image_bytes: bytes) -> bytes:
    image = Image.open(io.BytesIO(image_bytes))
    gray_image = image.convert('L')
    blurred_image = gray_image.filter(ImageFilter.GaussianBlur(radius=2))
    img_array = np.array(blurred_image)
    threshold = 100
    binary_array = (img_array > threshold) * 255
    result_image = Image.fromarray(binary_array.astype(np.uint8))
    output_buffer = io.BytesIO()
    result_image.save(output_buffer, format='PNG')
    output_buffer.seek(0)
    return output_buffer

@app.route('/process', methods=['POST'])
def process_image():
    if 'image' not in request.files:
        return "No image file uploaded", 400
    image_file = request.files['image']
    image_bytes = image_file.read()
    result_buffer = process_image_bytes(image_bytes)
    return send_file(result_buffer, mimetype='image/png')

if __name__ == '__main__':
    # Получаем порт из аргумента командной строки, по умолчанию 5000
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    app.run(host='0.0.0.0', port=port)

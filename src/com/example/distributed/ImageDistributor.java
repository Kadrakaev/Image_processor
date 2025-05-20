package com.example.distributed;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.Graphics2D;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.*;
import java.util.concurrent.*;

/**
 * Пример параллельной отправки частей изображения на Python-воркеры.
 */
public class ImageDistributor {

    // Адреса Python-воркеров, на которые отправляем части.
    private static final List<String> WORKERS = List.of(
            "http://localhost:5000/process_chunk",
            "http://localhost:5001/process_chunk",
            "http://localhost:5002/process_chunk"
    );

    // Количество строк и столбцов для разбиения изображения
    private static final int NUM_ROWS = 2;
    private static final int NUM_COLS = 2;

    public static void main(String[] args) throws Exception {
        // Шаг 1: Загрузка исходного изображения из файла
        BufferedImage image = ImageIO.read(new File("input.jpg"));
        System.out.println("Исходное изображение загружено: " + image.getWidth() + "x" + image.getHeight());

        // Вычисляем размеры части (тайла)
        int tileWidth = image.getWidth() / NUM_COLS;
        int tileHeight = image.getHeight() / NUM_ROWS;

        // Создаём пул потоков для параллельной отправки и обработки
        ExecutorService executor = Executors.newFixedThreadPool(NUM_ROWS * NUM_COLS);

        // Список для хранения будущих результатов (Future) каждой отправки
        List<Future<BufferedImage>> futures = new ArrayList<>();

        // Переменная для выбора Python-воркера по кругу (round-robin)
        int workerIndex = 0;

        // Шаг 2: Разбиваем исходное изображение на тайлы и отправляем на обработку
        for (int y = 0; y < NUM_ROWS; y++) {
            for (int x = 0; x < NUM_COLS; x++) {
                // Текущие координаты тайла
                int finalX = x;
                int finalY = y;

                // Выбираем адрес Python-воркера по кругу
                String workerUrl = WORKERS.get(workerIndex % WORKERS.size());
                workerIndex++;

                // Создаём тайл - подизображение
                BufferedImage chunk = image.getSubimage(
                        finalX * tileWidth,
                        finalY * tileHeight,
                        tileWidth,
                        tileHeight
                );

                // Отправляем обработку в отдельном потоке
                Future<BufferedImage> future = executor.submit(() -> sendChunk(workerUrl, chunk));
                futures.add(future);
            }
        }

        // Шаг 3: Создаём пустое итоговое изображение того же размера
        BufferedImage result = new BufferedImage(
                image.getWidth(),
                image.getHeight(),
                BufferedImage.TYPE_INT_RGB
        );
        Graphics2D g = result.createGraphics();

        // Шаг 4: Получаем результаты из потоков и собираем в итоговое изображение
        int i = 0;
        for (int y = 0; y < NUM_ROWS; y++) {
            for (int x = 0; x < NUM_COLS; x++) {
                try {
                    // Получаем обработанный тайл (блокируем, если не готов)
                    BufferedImage processedChunk = futures.get(i++).get();

                    // Рисуем обработанный тайл в итоговое изображение
                    g.drawImage(processedChunk, x * tileWidth, y * tileHeight, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Ошибка при получении обработанного блока.");
                }
            }
        }
        g.dispose();

        // Шаг 5: Сохраняем итоговое изображение в файл
        ImageIO.write(result, "jpg", new File("output.jpg"));
        System.out.println("Обработанное изображение сохранено в output.jpg");

        // Завершаем пул потоков
        executor.shutdown();
    }

    /**
     * Отправляет часть изображения на Python-воркер по HTTP POST,
     * получает обратно обработанное изображение.
     *
     * @param url   Адрес Python-воркера
     * @param chunk Подизображение (тайл) для обработки
     * @return Обработанное изображение (BufferedImage)
     * @throws Exception При ошибках сети или обработки
     */
    private static BufferedImage sendChunk(String url, BufferedImage chunk) throws Exception {
        // Преобразуем BufferedImage в байты JPEG
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(chunk, "jpg", baos);
        byte[] imageBytes = baos.toByteArray();

        // Создаём HTTP клиент (Java 11+)
        HttpClient client = HttpClient.newHttpClient();

        // Формируем POST запрос с бинарными данными изображения
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/octet-stream")
                .POST(BodyPublishers.ofByteArray(imageBytes))
                .build();

        // Отправляем запрос и получаем ответ как InputStream
        HttpResponse<InputStream> response = client.send(request, BodyHandlers.ofInputStream());

        // Если ответ успешный (код 200), читаем изображение из потока
        if (response.statusCode() == 200) {
            return ImageIO.read(response.body());
        } else {
            throw new IOException("Ошибка от сервера " + url + ": HTTP " + response.statusCode());
        }
    }
}

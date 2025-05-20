package com.example.distributed;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.Graphics2D;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class ImageDistributor {

    // Логгер для вывода сообщений и ошибок
    private static final Logger logger = Logger.getLogger(ImageDistributor.class.getName());

    // Список URL Python-воркеров для отправки частей изображения
    private static final List<String> WORKERS = List.of(
            "http://localhost:5000/process",
            "http://localhost:5001/process",
            "http://localhost:5002/process"
    );

    // Конфигурация: сколько строк и столбцов частей сделать из исходного изображения
    private static final int NUM_ROWS = 2;
    private static final int NUM_COLS = 2;

    public static void main(String[] args) throws Exception {
        configureLogger();  // Настраиваем логгер (только в файл)

        // Загружаем исходное изображение из файла
        BufferedImage image = ImageIO.read(new File("C:\\Users\\Acer\\IdeaProjects\\Image_processor\\src\\image1.jpg"));
        logger.info("Исходное изображение загружено: " + image.getWidth() + "x" + image.getHeight());

        // Вычисляем ширину и высоту каждого тайла (части)
        int tileWidth = image.getWidth() / NUM_COLS;
        int tileHeight = image.getHeight() / NUM_ROWS;

        // Создаём пул потоков для параллельной обработки (каждый тайл — в отдельном потоке)
        ExecutorService executor = Executors.newFixedThreadPool(NUM_ROWS * NUM_COLS);

        // Список для хранения Future — результата асинхронной отправки чанков на обработку
        List<Future<BufferedImage>> futures = new ArrayList<>();

        int workerIndex = 0;  // Для выбора воркера по кругу (round-robin)

        // Разбиваем исходное изображение на части и отправляем их на обработку
        for (int y = 0; y < NUM_ROWS; y++) {
            for (int x = 0; x < NUM_COLS; x++) {
                int finalX = x;  // Нужно для лямбды, чтобы сохранить значение
                int finalY = y;

                // Выбираем воркер по кругу из списка
                String workerUrl = WORKERS.get(workerIndex % WORKERS.size());
                workerIndex++;

                // Вырезаем из исходного изображения тайл — часть по координатам и размеру
                BufferedImage chunk = image.getSubimage(
                        finalX * tileWidth,
                        finalY * tileHeight,
                        tileWidth,
                        tileHeight
                );

                // Отправляем тайл на обработку в отдельном потоке с повторными попытками
                futures.add(executor.submit(() -> {
                    try {
                        return sendChunkWithRetry(workerUrl, chunk, 3);
                    } catch (Exception e) {
                        logger.severe("Не удалось обработать часть изображения: " + e.getMessage());
                        throw e;  // Пробрасываем дальше, чтобы Future знал об ошибке
                    }
                }));
            }
        }

        // Создаём итоговое пустое изображение нужного размера
        BufferedImage result = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
        Graphics2D g = result.createGraphics();

        int i = 0;
        // Получаем из Future обработанные части и рисуем их в итоговое изображение
        for (int y = 0; y < NUM_ROWS; y++) {
            for (int x = 0; x < NUM_COLS; x++) {
                try {
                    BufferedImage processedChunk = futures.get(i++).get();  // Блокируем, ждём результат
                    g.drawImage(processedChunk, x * tileWidth, y * tileHeight, null);  // Рисуем в нужном месте
                } catch (Exception e) {
                    logger.severe("Ошибка при получении обработанного блока: " + e.getMessage());
                    // Можно отрисовать исходный чанк или оставить место пустым
                }
            }
        }
        g.dispose();  // Освобождаем ресурсы графики

        // Сохраняем итоговое изображение в файл
        ImageIO.write(result, "jpg", new File("output.jpg"));
        logger.info("Обработанное изображение сохранено в output.jpg");

        executor.shutdown();  // Завершаем пул потоков
    }

    /**
     * Отправка чанка с повторными попытками.
     * maxRetries — максимальное число попыток отправить часть.
     * При ошибке отправки ждет и увеличивает задержку (экспоненциальная backoff).
     */
    private static BufferedImage sendChunkWithRetry(String url, BufferedImage chunk, int maxRetries) throws Exception {
        int attempt = 0;
        long delay = 1000; // начальная задержка в миллисекундах (1 секунда)
        Exception lastException = null;

        while (attempt < maxRetries) {
            try {
                return sendChunk(url, chunk);  // Пытаемся отправить и получить результат
            } catch (IOException e) {
                attempt++;
                lastException = e;
                logger.warning("Попытка " + attempt + " отправки части на " + url + " завершилась ошибкой: " + e.getMessage());
                if (attempt < maxRetries) {
                    logger.info("Ждем " + delay + " мс перед повторной попыткой...");
                    Thread.sleep(delay);  // ждем перед повторной попыткой
                    delay *= 2;  // увеличиваем задержку в 2 раза (экспоненциально)
                }
            }
        }
        throw new IOException("Не удалось отправить часть после " + maxRetries + " попыток", lastException);
    }

    /**
     * Отправка части изображения (чанка) на Python-воркер.
     * Конвертирует изображение в JPEG байты, отправляет POST-запрос и читает ответ.
     */
    private static BufferedImage sendChunk(String url, BufferedImage chunk) throws Exception {
        // Конвертируем BufferedImage в байты JPEG
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(chunk, "jpg", baos);
        byte[] imageBytes = baos.toByteArray();

        // Создаем HTTP клиент с таймаутом подключения
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        // Формируем POST-запрос с содержимым байтов изображения
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/octet-stream")  // бинарные данные
                .timeout(Duration.ofSeconds(10))  // таймаут ожидания ответа
                .POST(BodyPublishers.ofByteArray(imageBytes))
                .build();

        // Отправляем запрос, ожидаем InputStream в ответе
        HttpResponse<InputStream> response = client.send(request, BodyHandlers.ofInputStream());

        // Если ответ успешный, читаем изображение из входящего потока
        if (response.statusCode() == 200) {
            return ImageIO.read(response.body());
        } else {
            // Если ошибка, бросаем исключение с описанием
            throw new IOException("Ошибка от сервера " + url + ": HTTP " + response.statusCode());
        }
    }

    /**
     * Настройка логгера: вывод всех сообщений только в файл worker.log,
     * без вывода в консоль.
     */
    private static void configureLogger() throws IOException {
        Logger rootLogger = Logger.getLogger("");

        // Удаляем все существующие обработчики
        for (Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }

        // Создаем файловый обработчик с ротацией: max 5 Мб, 3 файла бэкапа
        FileHandler fileHandler = new FileHandler("worker.log", 5 * 1024 * 1024, 3, true);
        fileHandler.setLevel(Level.ALL);

        // Форматтер для более читаемого лога с датой и временем
        SimpleFormatter formatter = new SimpleFormatter();
        fileHandler.setFormatter(formatter);

        // Добавляем файловый обработчик к root логгеру
        rootLogger.addHandler(fileHandler);

        // Устанавливаем уровень логирования на максимум (чтобы всё логировалось)
        rootLogger.setLevel(Level.ALL);
    }
}


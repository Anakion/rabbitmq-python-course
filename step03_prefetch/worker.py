"""Шаг 3. Получатель с ограничением предвыборки сообщений (prefetch_count=1).

Запуск:
  python worker.py [имя_воркера] [время_обработки_в_сек]

Примеры запуска нескольких воркеров в РАЗНЫХ терминалах:
  Терминал 1 (быстрый):   python worker.py "Бариста 1 (быстрый)" 1
  Терминал 2 (медленный): python worker.py "Бариста 2 (медленный)" 3

Главное новшество шага 3:
  channel.set_qos(prefetch_count=1)

Без этой строки RabbitMQ использует слепой round-robin и вываливает сообщения
в буфер воркеров пачками. Медленный воркер захватит половину сообщений и будет
обрабатывать их долго, пока быстрый воркер простаивает без работы!

С prefetch_count=1 RabbitMQ отдаёт воркеру ровно ОДНО сообщение и ждёт ack.
Пока ack не отправлен — новых сообщений этот воркер не получит.
В итоге быстрый воркер успеет сделать больше заказов, а очередь будет
обрабатываться максимально эффективно («Fair dispatch» / честное распределение).
"""

import asyncio
import json
import sys
import time

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

# Адрес RabbitMQ
RABBIT_URL = "amqp://guest:guest@localhost/"

# Имя очереди заказов (должно совпадать с send_order.py)
QUEUE_NAME = "orders"


async def main() -> None:
    # Имя воркера (по умолчанию "Бариста 1")
    worker_name = sys.argv[1] if len(sys.argv) > 1 else "Бариста 1"

    # Скорость обработки одного заказа в секундах (по умолчанию 2 сек)
    process_delay = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0

    # Счётчик, сколько заказов приготовил именно этот бариста за смену
    orders_processed_count = 0

    # 1. Подключаемся к RabbitMQ (TCP-соединение)
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Открываем канал внутри соединения
        channel = await connection.channel()

        # ══════════════════════════════════════════════════════════════
        # 🌟 КЛЮЧЕВАЯ СТРОКА ШАГА 3: QoS и prefetch_count=1
        # ══════════════════════════════════════════════════════════════
        #
        # QoS (Quality of Service) — настройка поведения канала.
        # prefetch_count=1 говорит серверу RabbitMQ:
        #
        # «Выдай мне только 1 сообщение. Не присылай следующее,
        #  пока я не пришлю ack за текущее!»
        #
        # ❌ БЕЗ ЭТОЙ СТРОКИ:
        #    RabbitMQ равномерно распихивает сообщения по воркерам (1-2-1-2...),
        #    даже если воркер 2 ещё занят тяжёлой задачей.
        #
        # ✅ С ЭТОЙ СТРОКОЙ:
        #    Сообщения ждут в очереди на брокере (Ready). Свободный воркер
        #    берёт заказ, готовит, шлёт ack — и только потом берёт следующий.
        # ══════════════════════════════════════════════════════════════
        await channel.set_qos(prefetch_count=1)

        # 3. Объявляем очередь (параметры обязаны совпадать с send_order.py)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        print(f"[{worker_name}] ☕ Вышел на смену! Скорость: {process_delay} сек/заказ.")
        print(f"[{worker_name}] 👂 Слушаю очередь '{QUEUE_NAME}'. Нажми Ctrl+C для выхода.\n")

        # 4. Читаем сообщения через итератор
        #    (напомним из шага 2: без no_ack, сообщения подтверждаются через ack)
        async with queue.iterator() as messages:
            async for message in messages:

                # message.process(requeue=True) отправляет ack в конце блока.
                # Если воркер упадёт или нажмёт Ctrl+C — отправится nack,
                # и заказ вернётся в очередь (requeue=True).
                async with message.process(requeue=True):
                    order = json.loads(message.body.decode())
                    order_id = order.get("order_id", "?")
                    drink = order.get("drink", "Напиток")

                    start_time = time.time()
                    print(f"[{worker_name}] ⏳ ВЗЯЛ заказ #{order_id} ({drink}). Готовлю...")

                    # Имитируем работу бариста (приготовление напитка)
                    await asyncio.sleep(process_delay)

                    elapsed = round(time.time() - start_time, 2)
                    orders_processed_count += 1

                    print(
                        f"[{worker_name}] ✅ ВЫДАЛ заказ #{order_id} ({drink}) за {elapsed} сек! "
                        f"[Всего выдано этим воркером: {orders_processed_count}]"
                    )

                # ← Здесь message.process() отправляет ack в RabbitMQ!
                # И ТОЛЬКО ПОСЛЕ ЭТОГО RabbitMQ пришлёт следующее сообщение (благодаря prefetch_count=1).


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Бариста] 🛑 Закончил смену, ушёл отдыхать.")
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)

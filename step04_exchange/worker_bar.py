"""Шаг 4. Воркер Бара (Бариста).

Запуск:  python worker_bar.py
Остановка: Ctrl+C

Что делает этот воркер:
1. Объявляет обменник 'cafe_orders' (DIRECT).
2. Объявляет очередь 'bar_orders'.
3. Привязывает (bind) свою очередь к обменнику с routing_key='bar'.
4. Принимает и готовит ТОЛЬКО напитки. Еда сюда никогда не попадёт!
"""

import asyncio
import json
import sys
import time

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

RABBIT_URL = "amqp://guest:guest@localhost/"
EXCHANGE_NAME = "cafe_orders"
QUEUE_NAME = "bar_orders"
ROUTING_KEY = "bar"


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        channel = await connection.channel()

        # Балансировка: берём ровно по 1 задаче (из шага 3)
        await channel.set_qos(prefetch_count=1)

        # 1. Объявляем Exchange (тот же самый, что и у отправителя)
        exchange = await channel.declare_exchange(
            EXCHANGE_NAME,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        # 2. Объявляем очередь бара
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        # ══════════════════════════════════════════════════════════════
        # 🌟 КЛЮЧЕВАЯ СТРОКА ШАГА 4: Binding (привязка очереди к обменнику)
        # ══════════════════════════════════════════════════════════════
        # Сообщаем RabbitMQ: «Пересылай в очередь bar_orders только те сообщения
        # из обменника cafe_orders, у которых routing_key равен 'bar'»
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        print(f"☕ [Бариста] Вышел за стойку бара!")
        print(f"☕ [Бариста] Слушаю очередь '{QUEUE_NAME}' (привязана к '{EXCHANGE_NAME}' по ключу '{ROUTING_KEY}')")
        print(f"☕ [Бариста] Нажми Ctrl+C для выхода.\n")

        async with queue.iterator() as messages:
            async for message in messages:
                # ack отправляется по завершении блока (из шага 2)
                async with message.process(requeue=True):
                    order = json.loads(message.body.decode())
                    order_id = order.get("order_id")
                    item = order.get("item")

                    start_time = time.time()
                    print(f"☕ [Бариста] ⏳ ВЗЯЛ заказ #{order_id} ({item}). Варю кофе...")

                    # Имитируем приготовление напитка (1.5 секунды)
                    await asyncio.sleep(1.5)

                    elapsed = round(time.time() - start_time, 2)
                    print(f"☕ [Бариста] ✅ Напиток #{order_id} ({item}) готов за {elapsed}с! (ack отправлен)\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n☕ [Бариста] Смена окончена, ушёл отдыхать.")
        sys.exit(0)
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Проверь: docker compose up -d")
        sys.exit(1)

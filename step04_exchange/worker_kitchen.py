"""Шаг 4. Воркер Кухни (Повар).

Запуск:  python worker_kitchen.py
Остановка: Ctrl+C

Что делает этот воркер:
1. Объявляет обменник 'cafe_orders' (DIRECT).
2. Объявляет очередь 'kitchen_orders'.
3. Привязывает (bind) свою очередь к обменнику с routing_key='kitchen'.
4. Принимает и готовит ТОЛЬКО блюда кухни (еду). Напитки сюда никогда не попадёт!
"""

import asyncio
import json
import sys
import time

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

RABBIT_URL = "amqp://guest:guest@localhost/"
EXCHANGE_NAME = "cafe_orders"
QUEUE_NAME = "kitchen_orders"
ROUTING_KEY = "kitchen"


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

        # 2. Объявляем очередь кухни
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        # ══════════════════════════════════════════════════════════════
        # 🌟 КЛЮЧЕВАЯ СТРОКА ШАГА 4: Binding (привязка очереди к обменнику)
        # ══════════════════════════════════════════════════════════════
        # Сообщаем RabbitMQ: «Пересылай в очередь kitchen_orders только те сообщения
        # из обменника cafe_orders, у которых routing_key равен 'kitchen'»
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        print(f"🍳 [Повар] Заступил на смену на кухне!")
        print(f"🍳 [Повар] Слушаю очередь '{QUEUE_NAME}' (привязана к '{EXCHANGE_NAME}' по ключу '{ROUTING_KEY}')")
        print(f"🍳 [Повар] Нажми Ctrl+C для выхода.\n")

        async with queue.iterator() as messages:
            async for message in messages:
                # ack отправляется по завершении блока (из шага 2)
                async with message.process(requeue=True):
                    order = json.loads(message.body.decode())
                    order_id = order.get("order_id")
                    item = order.get("item")

                    start_time = time.time()
                    print(f"🍳 [Повар] ⏳ ВЗЯЛ заказ #{order_id} ({item}). Готовлю еду...")

                    # Приготовление блюда на кухне занимает чуть больше времени (2.5 секунды)
                    await asyncio.sleep(2.5)

                    elapsed = round(time.time() - start_time, 2)
                    print(f"🍳 [Повар] ✅ Блюдо #{order_id} ({item}) готово за {elapsed}с! (ack отправлен)\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🍳 [Повар] Смена окончена, плита выключена.")
        sys.exit(0)
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Проверь: docker compose up -d")
        sys.exit(1)

"""Шаг 3. Отправитель: кассир отправляет пачку заказов.

Запуск:  python send_order.py [количество_заказов]
Пример:  python send_order.py 6

В этом шаге мы отправляем сразу несколько заказов подряд,
чтобы увидеть, как они будут распределяться между несколькими
воркерами (бариста).
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

# Адрес RabbitMQ (протокол://логин:пароль@хост/)
RABBIT_URL = "amqp://guest:guest@localhost/"

# Имя очереди заказов
QUEUE_NAME = "orders"

# Примеры напитков разной сложности для наглядности
DRINKS = [
    "Эспрессо",
    "Капучино",
    "Сложный авторский Раф с сиропом",
    "Фильтр-кофе",
    "Флэт Уайт",
    "Бамбл-кофе со льдом",
]


async def main() -> None:
    # Сколько заказов отправить (по умолчанию 6)
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 6

    # 1. Подключаемся к RabbitMQ (одно общее соединение)
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Создаём канал для передачи сообщений
        channel = await connection.channel()

        # 3. Объявляем очередь (durable=True обязательно для RabbitMQ 4)
        await channel.declare_queue(QUEUE_NAME, durable=True)

        print(f"Кассир начинает отправку {count} заказов в очередь '{QUEUE_NAME}'...\n")

        # 4. Отправляем заказы один за другим
        for i in range(1, count + 1):
            drink = DRINKS[(i - 1) % len(DRINKS)]
            order = {
                "order_id": i,
                "drink": drink,
                "email": f"client_{i}@example.com",
            }

            # Сериализуем словарь в JSON-байты
            body = json.dumps(order, ensure_ascii=False).encode()
            message = aio_pika.Message(body=body)

            # Отправляем сообщение в очередь через default exchange
            await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)
            print(f"  [Кассир] 📨 Заказ #{i} ({drink}) отправлен на стойку")

            # Небольшая пауза в 0.1 сек для реалистичности отправки
            await asyncio.sleep(0.1)

        print(f"\nВсе {count} заказов успешно отправлены!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)

"""Шаг 4. Отправитель: кассир отправляет заказы в Exchange с маршрутизацией.

Запуск:  python send_order.py
Пример с аргументом: python send_order.py 8

Главное новшество шага 4:
  Вместо отправки напрямую в очередь (через channel.default_exchange):
    await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)
  Мы отправляем заказ в наш собственный ОБМЕННИК (Exchange):
    await exchange.publish(message, routing_key="bar" | "kitchen")

Обменник сам решит, в какую очередь (или очереди) направить сообщение,
основываясь на routing_key и привязках (bindings).
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

# Адрес RabbitMQ
RABBIT_URL = "amqp://guest:guest@localhost/"

# Имя нашего собственного обменника типа Direct
EXCHANGE_NAME = "cafe_orders"

# Меню кафе с разделением по категориям (целевым цехам)
MENU_ITEMS = [
    {"name": "Капучино", "dept": "bar", "routing_key": "bar"},
    {"name": "Круассан с шоколадом", "dept": "kitchen", "routing_key": "kitchen"},
    {"name": "Флэт Уайт", "dept": "bar", "routing_key": "bar"},
    {"name": "Сэндвич с лососем", "dept": "kitchen", "routing_key": "kitchen"},
    {"name": "Бамбл-кофе со льдом", "dept": "bar", "routing_key": "bar"},
    {"name": "Сырники со сметаной", "dept": "kitchen", "routing_key": "kitchen"},
]


async def main() -> None:
    # Сколько заказов отправить (по умолчанию по числу позиций меню: 6)
    count = int(sys.argv[1]) if len(sys.argv) > 1 else len(MENU_ITEMS)

    # 1. Подключаемся к RabbitMQ
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Создаём канал
        channel = await connection.channel()

        # 3. Объявляем Exchange типа DIRECT (durable=True обязателен для сохранности)
        exchange = await channel.declare_exchange(
            EXCHANGE_NAME,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        print(f"Кассир подключился к обменнику '{EXCHANGE_NAME}' (тип: DIRECT).")
        print(f"Начинаем отправку {count} заказов...\n")

        # 4. Отправляем заказы
        for i in range(1, count + 1):
            item = MENU_ITEMS[(i - 1) % len(MENU_ITEMS)]
            routing_key = item["routing_key"]

            order = {
                "order_id": i,
                "item": item["name"],
                "department": item["dept"],
            }

            # Сериализуем данные в JSON
            body = json.dumps(order, ensure_ascii=False).encode()
            message = aio_pika.Message(
                body=body,
                content_type="application/json",
            )

            # ══════════════════════════════════════════════════════════════
            # 🌟 КЛЮЧЕВАЯ СТРОКА ШАГА 4: Публикация в Exchange с routing_key
            # ══════════════════════════════════════════════════════════════
            await exchange.publish(message, routing_key=routing_key)

            icon = "☕" if routing_key == "bar" else "🍳"
            print(
                f"  [Кассир] 📨 Заказ #{i:02d}: {icon} '{item['name']}' "
                f"-> Exchange '{EXCHANGE_NAME}' [routing_key='{routing_key}']"
            )

            # Пауза в 0.2 сек для наглядности вывода
            await asyncio.sleep(0.2)

        print(f"\nВсе {count} заказов успешно отправлены в обменник!\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Проверь: docker compose up -d")
        sys.exit(1)

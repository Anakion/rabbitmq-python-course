# ☕ OrdeFflow — Практический курс по RabbitMQ на Python (`aio-pika`)

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![aio-pika](https://img.shields.io/badge/aio--pika-≥9.4-brightgreen.svg)](https://aio-pika.readthedocs.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.0%20(Management)-orange.svg)](https://www.rabbitmq.com/)
[![Docker](https://img.shields.io/badge/Docker-compose-2496ED.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Пошаговый курс по **RabbitMQ** для Python-бэкендеров без «воды» и занудной теории.  
Вместо абстрактных примеров весь курс построен вокруг живой симуляции **городского кафе**:
* **Кассир** принимает заказы у гостей и отправляет их в систему (**Producer**).
* **Стойка заказов** накапливает чеки (**Queues**).
* **Менеджер раздачи** распределяет заказы: напитки — в бар, еду — на кухню (**Exchange & Routing**).
* **Бариста и Повар** готовят заказы в своём темпе (**Consumers / Workers**).

Каждый шаг посвящён **ровно одному ключевому паттерну**, который используется в реальном продакшне.

---

## 🛠️ Стек технологий

* **Язык:** Python 3.10+ (совместимо с Python 3.14)
* **Библиотека AMQP:** `aio-pika` ≥ 9.4 (асинхронная, быстрая, нативная поддержка `asyncio`)
* **Брокер сообщений:** RabbitMQ 4 (с активным `management`-плагином)
* **Контейнеризация:** Docker & Docker Compose
* **Финал курса:** Микросервис на FastAPI + PostgreSQL + SQLAlchemy 2.0 (async) + RabbitMQ

---

## 🚀 Быстрый старт

### 1. Клонирование репозитория
```bash
git clone https://github.com/Anakion/rabbitmq-python-course.git
cd rabbitmq-python-course
```

### 2. Запуск RabbitMQ в Docker
В корне проекта уже подготовлен `docker-compose.yml`:
```bash
docker compose up -d
```
* **Порт `5672`:** протокол AMQP (сюда подключаются Python-скрипты).
* **Порт `15672`:** веб-панель управления RabbitMQ Management:
  * URL: [http://localhost:15672](http://localhost:15672)
  * Логин: `guest`
  * Пароль: `guest`

### 3. Установка Python-зависимостей
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 🗺️ Дорожная карта курса

### 📚 Блок 1. Основы RabbitMQ на чистом Python (`asyncio` + `aio-pika`)

| Шаг | Тема | Паттерн / Решаемая проблема | Статус |
|:---:|:---|:---|:---:|
| **01** | [**Первое сообщение**](./step01_first_message/README.md) | Producer, Consumer, базовая очередь `orders`. Связь скриптов через брокер. | ✅ Готов |
| **02** | [**Подтверждение доставки (ACK)**](./step02_ack/README.md) | `message.process(requeue=True)`. Защита от потери заказов при падении воркера. | ✅ Готов |
| **03** | [**Честное распределение (Prefetch)**](./step03_prefetch/README.md) | `channel.set_qos(prefetch_count=1)`. Балансировка между быстрым и медленным бариста. | ✅ Готов |
| **04** | [**Обменники и маршрутизация**](./step04_exchange/README.md) | `Direct Exchange`, `routing_key`, `bindings`. Разделение потоков: напитки → бар, еда → кухня. | ✅ Готов |
| **05** | **Битые сообщения (DLQ)** | `Dead Letter Queue`, `x-dead-letter-exchange`. Изоляция «ядовитых» заказов без остановки воркеров. | 🔜 В работе |
| **06** | **Надёжность и персистентность** | `DeliveryMode.PERSISTENT`, durable queues. Сохранение сообщений при перезагрузке брокера. | 🔜 В плане |

---

### 🏗️ Блок 2. Полноценный пет-проект в продакшн-архитектуре

| Шаг | Тема | Описание архитектуры | Статус |
|:---:|:---|:---|:---:|
| **07** | **Асинхронный сервис заказов** | **FastAPI + PostgreSQL + RabbitMQ + Docker Compose**<br>• `POST /orders` мгновенно ставит задачу в RabbitMQ<br>• Фоновые воркеры готовят заказ и обновляют статус в БД<br>• `GET /orders/{id}` отслеживает статус готовности | 🔜 Финал |

```
[ Клиент ] ──(HTTP POST)──► [ FastAPI API ] ──(AMQP)──► [ RabbitMQ ]
                                                              │
                                                              ▼
[ Клиент ] ◄──(Статус)───── [ PostgreSQL ] ◄──(Save)─── [ Worker ]
```

---

## 💡 Важные особенности курса

1. **Совместимость с RabbitMQ 4:** в RabbitMQ 4 запрещены `transient` неэксклюзивные очереди, поэтому во всех уроках используется актуальный стандарт `declare_queue(durable=True)`.
2. **Асинхронность из коробки:** обучение строится сразу на современном асинхронном стеке `aio-pika`, без устаревших блокирующих подходов.
3. **Понятная визуализация:** каждый шаг сопровождается разбором метрик в RabbitMQ Management UI (`Ready`, `Unacked`, `Total`, схемы `Bindings`).
4. **Готовые эксперименты:** в каждом шаге есть готовый сценарий с несколькими терминалами, позволяющий вживую увидеть сбои и их решения.

---

## 🤝 Контрибьютинг и обратная связь

Если нашёл опечатку или хочешь предложить улучшение — смело открывай **Issue** или присылай **Pull Request**! ⭐ Ставь звезду проекту, если материалы оказались полезными.

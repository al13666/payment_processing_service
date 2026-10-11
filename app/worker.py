import os
import time

import pika
import psycopg2


def callback(channel, method, properties, body):
    task_id, message = body.decode().split(":", 1)

    print(f"Processing task {task_id}: {message}", flush=True)

    with psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        dbname=os.getenv("DB_NAME", "demo"),
        user=os.getenv("DB_USER", "demo"),
        password=os.getenv("DB_PASSWORD", "demo"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, message FROM tasks WHERE id = %s",
                (int(task_id),),
            )
            task = cur.fetchone()

    print(f"Task found in PostgreSQL: {task}", flush=True)

    channel.basic_ack(delivery_tag=method.delivery_tag)


while True:
    try:
        credentials = pika.PlainCredentials("demo", "demo123")
        parameters = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            credentials=credentials,
            connection_attempts=10,
            retry_delay=2,
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue="tasks", durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue="tasks",
            on_message_callback=callback,
        )

        print("Worker is waiting for messages...", flush=True)
        channel.start_consuming()

    except Exception as error:
        print(f"Worker connection error: {error}", flush=True)
        time.sleep(5)
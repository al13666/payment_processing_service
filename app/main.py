import os
import time

import pika
import psycopg2
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "demo"),
    "user": os.getenv("DB_USER", "demo"),
    "password": os.getenv("DB_PASSWORD", "demo"),
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    for attempt in range(30):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS tasks (
                            id SERIAL PRIMARY KEY,
                            message TEXT NOT NULL
                        )
                    """)
            return
        except psycopg2.OperationalError:
            if attempt == 29:
                raise
            time.sleep(2)


def publish_task(task_id: int, message: str):
    credentials = pika.PlainCredentials("demo", "demo123")
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        credentials=credentials,
        connection_attempts=10,
        retry_delay=2,
    )

    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.queue_declare(queue="tasks", durable=True)
        channel.basic_publish(
            exchange="",
            routing_key="tasks",
            body=f"{task_id}:{message}".encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        connection.close()


class TaskCreate(BaseModel):
    message: str


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/tasks")
def create_task(task: TaskCreate):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks (message) VALUES (%s) RETURNING id",
                (task.message,),
            )
            task_id = cur.fetchone()[0]

    publish_task(task_id, task.message)

    return {"id": task_id, "message": task.message}


@app.get("/tasks")
def get_tasks():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, message FROM tasks ORDER BY id")
            rows = cur.fetchall()

    return [
        {"id": row[0], "message": row[1]}
        for row in rows
    ]
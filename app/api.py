# app/api.py

import json
import asyncio
from fastapi import APIRouter, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from aiokafka import AIOKafkaConsumer

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    consumer = AIOKafkaConsumer(
        "crypto_prices",  # Make sure this matches your Kafka topic
        bootstrap_servers="localhost:9092",
        group_id="fastapi-ui",
        auto_offset_reset="latest"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await websocket.send_text(msg.value.decode("utf-8"))
    except Exception as e:
        print(f"WebSocket Error: {e}")
    finally:
        await consumer.stop()

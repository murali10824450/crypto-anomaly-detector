# app/main.py

from fastapi import FastAPI
from app import api

app = FastAPI()
app.include_router(api.router)

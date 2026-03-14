from fastapi import FastAPI
from app.api.endpoints import chat

app = FastAPI()

app.include_router(chat.router, prefix="/api/v1", tags=["chat"])

@app.get("/")
def read_root():
    return {"Hello": "World"}

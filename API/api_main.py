from fastapi import FastAPI
from confluent_kafka import Producer
import hashlib
from sqlmodel import SQLModel, Field




app = FastAPI()


conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'client.id' : "1",
    'acks' : '0'
}
prod = Producer(conf)






@app.get('/')
async def root():
    return {"data" : "Whatcha doing here foo"}


@app.post("/query")
async def inference(prompt):
    prod.produce("input_prompts", prompt)
    m = hashlib.sha256()
    m.update(prompt.encode("utf-8"))
    return {
        "data" : prompt,
        "hash" : m.hexdigest()
    }


@app.get("/query/{prompt_id}")
async def view_data(prompt_id : str):
    return {
        "don't know ? " : "Use this path to retrieve the stored data", 
        "id" : f"{prompt_id}"
        }
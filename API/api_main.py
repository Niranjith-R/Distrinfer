from fastapi import FastAPI
from confluent_kafka import Producer
import hashlib
from sqlmodel import SQLModel, Field
from enum import Enum




app = FastAPI()


conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'client.id' : "1",
    'acks' : '0'
}
prod = Producer(conf)


class Status(Enum):
    Success = 1
    Pending = 2
    Failed = 3


class Data(SQLModel, table = True):
    id : str = Field(default = None, primary_key= True)
    prompt : str = Field(nullable = False)
    infer : str = Field(default = "-")
    status : Status = Field(default = Status.Pending, nullable = False)

class User(SQLModel, table = True):
    id : int = Field(default = None, primary_key = True)
    username : str = Field(nullable = False)
    


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
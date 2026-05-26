from fastapi import FastAPI
from confluent_kafka import Producer
import hashlib
from sqlmodel import SQLModel, Field, Relationship, create_engine
from enum import Enum
import time




app = FastAPI()


conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'client.id' : "1",
    'acks' : '0'
}
prod = Producer(conf)


DATABASE_URL = "postgresql://postgres:sarangi@192.168.1.8:5432/Distrinfer"
engine = create_engine(DATABASE_URL, echo = True)


class Status(Enum):
    Success = 1
    Pending = 2
    Failed = 3


class Data(SQLModel, table = True):
    id : str = Field(default = None, primary_key= True)
    prompt : str = Field(nullable = False)
    infer : str = Field(default = "-")
    status : Status = Field(default = Status.Pending, nullable = False)

    
    User_id : int = Field(default = None, foreign_key = "user.id")
    user : User = Relationship(back_populates = "user")

class User(SQLModel, table = True):
    id : int = Field(default = None, primary_key = True)
    username : str = Field(nullable = False)
    # Find the proper way to store passwords
    # Found it, Argon2id
    passwrd : str = Field(default = None, nullable = False)
    UID : int = Field(default = None)


def create_table():
    SQLModel.metadata.create_all(engine)


@app.on_event("startup")
def on_startup():
    create_table()



@app.get('/')
async def root():
    return {"data" : "Whatcha doing here foo"}


@app.post("/query")
async def inference(prompt):
    prod.produce("input_prompts", prompt)
    m = hashlib.sha256()
    m.update(prompt.encode("utf-8"))
    m.update(str(time.time()).encode("utf-8"))
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



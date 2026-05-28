from fastapi import FastAPI, Depends
from typing import Annotated
from confluent_kafka import Producer
import hashlib
from sqlmodel import SQLModel, Field, Relationship, create_engine, Session
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
    id: int | None = Field(default=None, primary_key=True)
    prompt : str = Field(nullable = False)
    infer : str = Field(default = "-")
    status : Status = Field(default = Status.Pending, nullable = False)
    Hash : str

    
    # User_id : int = Field(default = None, foreign_key = "user.id")
    # user : User = Relationship(back_populates = "User")

class User(SQLModel, table = True):
    id : int = Field(default = None, primary_key = True)
    username : str = Field(nullable = False)
    # Find the proper way to store passwords
    # Found it, Argon2id
    passwrd : str = Field(default = None, nullable = False)
    UID : int = Field(default = None)


def create_table():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session



Session_dep = Annotated[Session, Depends(get_session)]


@app.on_event("startup")
def on_startup():
    create_table()



@app.get('/')
async def root():
    return {"data" : "Whatcha doing here foo"}


@app.post("/query")
async def inference(prompt : Data, session : Session_dep):
    prod.produce("input_prompts", prompt.prompt.encode("utf-8"))
    m = hashlib.sha256()
    m.update(prompt.prompt.encode("utf-8"))
    m.update(str(time.time()).encode("utf-8"))
    hex = m.hexdigest()
    prompt.Hash = hex
    session.add(prompt)
    session.commit()
    session.refresh(prompt)
    return {
        "data" : {
            "prompt" : prompt.prompt,
            "status" : prompt.status,
        },
        "hash" : hex
    }


@app.get("/query/{prompt_id}")
async def view_data(prompt_id : str):
    return {
        "don't know ? " : "Use this path to retrieve the stored data", 
        "id" : f"{prompt_id}"
        }



from fastapi import FastAPI, Depends, HTTPException
from typing import Annotated
from confluent_kafka import Producer
import hashlib
from sqlmodel import SQLModel, Field, create_engine, Session, select
import kafka_admin
from enum import Enum
import time
import json




app = FastAPI()


conf = {
    'bootstrap.servers' : '0.0.0.0:9092',
    'client.id' : "1",
    'acks' : 'all',
    'retries' : 3
}
prod = Producer(conf)


DATABASE_URL = "postgresql://postgres:sarangi@192.168.1.8:5432/Distrinfer"
engine = create_engine(DATABASE_URL, echo = False)


class Status(Enum):
    Success = "Success"
    Pending = "Pending"
    Failed = "Failed"


class Data(SQLModel, table = True):
    id: int | None = Field(default=None, primary_key=True)
    prompt : str = Field(nullable = False)
    infer : str = Field(default = "-")
    status : Status = Field(default = Status.Pending, nullable = False)
    host: str | None = Field(default=None, nullable=True)
    hash : str = Field(default=None, nullable= True)
    UID : int = Field(default = None, nullable = False)
    
    # User_id : int = Field(default = None, foreign_key = "user.id")
    # user : User = Relationship(back_populates = "User")

class User(SQLModel, table = True):
    id : int = Field(default = None, primary_key = True, nullable = False)
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
    kafka_admin.run()
    # Call kafka_admin.py to set the number of partitions to ensure concurrency.



@app.get('/')
async def root():
    return {"data" : "Whatcha doing here foo"}


@app.post("/query")
async def inference(prompt : Data, session : Session_dep):

    def delivery_report(err, msg):
        if err is not None:
            raise HTTPException(status_code=502, detail="Kafka Delivery Failed")

    m = hashlib.sha256()
    m.update(prompt.prompt.encode("utf-8"))
    m.update(str(time.time()).encode("utf-8"))
    hex = m.hexdigest()

    data = {
        "prompt" : prompt.prompt,
        "UID" : prompt.UID,
        "hex" :   hex
    }
    print(json.dumps(data))
    prod.produce("input_prompts", json.dumps(data).encode("utf-8"), callback = delivery_report)
    prod.poll(0)
    prod.flush(0)
    
    prompt.hash = hex
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
async def view_data(prompt_id : str, session : Session_dep):

    # Logic to Retrieve Data
    statement = select(Data).where(Data.hash == prompt_id)
    Results = session.exec(statement)
    for result in Results :
        return {
            # "don't know ? " : "Use this path to retrieve the stored data", 
            "id" : f"{prompt_id}",
            "status" : result.status,
            "Data" : {
                "prompt" : result.prompt,
                "infered" : result.infer
            }
            }


@app.get("/user")
async def list_users(session : Session_dep):
    statement = select(User)
    Users = session.exec(statement)
    data = []
    for user in Users:
        data.append(user)
    return {"Current Users" : data}


@app.post("/user")
async def create_user(content : User, session : Session_dep):
    session.add(content)
    session.commit()
    session.refresh(content)
    return content

@app.put("/user/{user}")
async def update_user(content : User, user : str, session : Session_dep):
    statement = select(User).where(User.UID == user)
    result = session.exec(statement)
    print("here")
    data_obj = result.one()
    data_cpy = data_obj.model_copy()
    data_obj.id = content.id
    data_obj.username = content.username
    data_obj.passwrd = content.passwrd
    data_obj.UID = content.UID
    print("Here")
    session.commit()
    session.refresh(data_obj)
    return {"old" : data_cpy,
            "new" : data_obj}

@app.delete("/user/{user}")
async def delete_user(user : int, session : Session_dep):
    statement = select(User).where(User.UID == user)
    result = session.exec(statement).first()
    if not result:
        raise HTTPException(status_code=404, detail= "Entry Not Found")
    session.delete(result)
    session.commit()
    return {"status" : "ok" }
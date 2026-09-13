from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from .Llama_Node.inference_node import infer, live
from typing import Annotated
import hashlib
from sqlmodel import SQLModel, Field, create_engine, Session, select
from sqlalchemy.dialects.postgresql import JSONB
from enum import Enum
import time
import json




app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # your Vite dev server URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DATABASE_URL = "postgresql://postgres:sarangi@192.168.1.8:5432/Distrinfer"
engine = create_engine(DATABASE_URL, echo = False)


class Status(Enum):
    Success = "Success"
    Pending = "Pending"
    Failed = "Failed"


class Data(SQLModel, table = True):
    id: int | None = Field(default=None, primary_key=True)
    prompt : str = Field(nullable = False)
    # infer : str = Field(default = "-")
    infer: dict = Field(default=None, sa_type=JSONB)
    status : Status = Field(default = Status.Pending, nullable = False)
    host: str | None = Field(default=None, nullable=True)
    hash : str = Field(default=None, nullable= True)


class Live_Model(SQLModel, table = False):
    prompt : str


def create_table():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session


Session_dep = Annotated[Session, Depends(get_session)]


@app.on_event("startup")
def on_startup():
    create_table()



@app.post("/")
def live_raw_data(prompt: Live_Model, session : Session_dep):

    infered_data = live.delay(prompt.prompt)

    while infered_data.ready() == False:
        time.sleep(0.1)
    return infered_data.get()


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
        "UID" : 1,
        "hex" :   hex
    }
    print(json.dumps(data))
    #Push to Celery
    infer.delay(json.dumps(data)) 
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
    statement = select(Data).where(Data.hash == prompt_id)
    Results = session.exec(statement)
    for result in Results :
        # return {
        #     "id" : f"{prompt_id}",
        #     "status" : result.status,
        #     "Data" : {
        #         "host" : result.host,
        #         "prompt" : result.prompt,
        #         "infered" : result.infer
        #     }
        #     }
        return result.infer
from celery import Celery
from celery.signals import worker_process_init
import psycopg2
from llama_cpp import Llama
import json
from socket import gethostname
from os import getenv



conn = curse = llm = None

#Add RabbitMQ connection details as broker and database url as Backend

app = Celery("inference_node",
              broker="- - - - - - -",
              backend="- - - - - - -"
             )


@worker_process_init.connect
def init_worker(**kwargs):
    global conn, curse, llm

    # Add you Database Connection here
    
    conn = psycopg2.connect(host = getenv("db_host", 'localhost'), dbname = getenv("db_name","Distrinfer"), user = getenv("db_user","postgres"), password = getenv("db_pass",'postgres'))
    curse = conn.cursor()
    llm = Llama.from_pretrained(
        repo_id=getenv("repo_id", default="Qwen/Qwen2.5-0.5B-Instruct-GGUF"),
        filename=getenv("filename", "*q8_0.gguf"),
        verbose = getenv("verbose", False),
        n_ctx = getenv("n_ctx", 32768),
    )


@app.task(name="inference_node.infer")
def infer(input):
    json_data = json.loads(input)
    hash = json_data.get("hex")
    prompt = json_data.get("prompt")
    response = llm.create_chat_completion(
            messages=[
                # {"role" : "system", "content" : "You are an assistant"},
                {"role" : "user", "content" : prompt},
            ],
            stream=False,
            max_tokens=512
        )
    
    update_infer_sql = "UPDATE DATA SET infer = %s, status = %s, host = %s WHERE hash = %s"
    curse.execute(update_infer_sql, (response["choices"][0]["message"]["content"], "Success", gethostname(), hash))
    conn.commit()


@app.task(name = "inference_node.live")
def live(input_prompt):
    response = llm.create_chat_completion(
        messages=[
            {"role" : "user", "content" : input_prompt}
        ],
        stream= False,
        max_tokens=1024
    )
    return response
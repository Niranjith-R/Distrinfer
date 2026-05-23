from fastapi import FastAPI


app = FastAPI()

@app.get('/')
async def root():
    return {"data" : "Whatcha doing here foo"}


@app.post("/query")
async def inference(prompt):
    return {
        "data" : prompt 
    }


@app.get("/query/{prompt_id}")
async def view_data(prompt_id : str):
    return {
        "don't know ? " : "Use this path to retrieve the stored data", 
        "id" : f"{prompt_id}"
        }
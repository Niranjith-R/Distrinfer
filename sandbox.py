from celery import Celery


app = Celery('tasks',broker="amqp://admin:admin@localhost:5672/")


print("This is a test")
@app.task
def add(x,y):
    print(x+y)
    return x+y


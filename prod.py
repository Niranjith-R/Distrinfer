from sandbox import add
from time import sleep


for i in range(100):
    add.delay(i, 0)

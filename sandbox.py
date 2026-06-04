# from argon2 import PasswordHasher
import psycopg2

# ph = PasswordHasher()

# hash = ph.hash("This is a test")

# print(hash)

# print(ph.verify(hash, "This is a test"))

conn = psycopg2.connect(host = "192.168.1.8", dbname = "Distrinfer", user = "postgres", password = 'sarangi')
curse = conn.cursor()

curse.execute("SELECT * FROM DATA")
print(curse.fetchall())
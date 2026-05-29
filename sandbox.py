from argon2 import PasswordHasher


ph = PasswordHasher()

hash = ph.hash("This is a test")

print(hash)

print(ph.verify(hash, "This is a test"))
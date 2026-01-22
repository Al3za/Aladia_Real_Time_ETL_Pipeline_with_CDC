# run venv\Scripts\activate at root project to run venv, and access imports modules
# activate to close venv
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# Carica le variabili dal file .env
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Creiamo l'engine SQLAlchemy
engine = create_engine(DATABASE_URL)

# Test: leggere dati dalla tabella Employee
with engine.connect() as conn:
    # result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")) list all sql tables 
    result = conn.execute(text('SELECT * FROM "Employee" LIMIT 5'))
    for row in result:
        print(row)

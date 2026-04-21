from fastapi import FastAPI
import psycopg2

app = FastAPI()

conn = psycopg2.connect(
    dbname="crypto",
    user="postgres",
    password="postgres",
    host="postgres"
)

@app.get("/prices")
def get_prices():
    cur = conn.cursor()
    cur.execute("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 50")
    rows = cur.fetchall()
    return rows

@app.get("/transactions")
def get_transactions():
    cur = conn.cursor()
    cur.execute("SELECT * FROM transactions ORDER BY timestamp DESC LIMIT 50")
    return cur.fetchall()
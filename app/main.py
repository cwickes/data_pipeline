from flask import Flask, request
import sqlite3

app = Flask(__name__)

def get_db_connection():
    con = sqlite3.connect('data/processed_data.db')
    con.row_factory = sqlite3.Row
    return con

def get_cols():
    con = get_db_connection()
    cur = con.execute('SELECT * FROM data')
    con.close()
    return list(map(lambda x: x[0], cur.description))

@app.route('/')
def home():
    return {
        "endpoints": [
            "/preview",
            "/fields",
            "/search",
            "/stats"
        ],
        "note": "Use /search?field=value to query data"
    }

@app.route('/preview')
def preview():
    con = get_db_connection()
    cur = con.execute('SELECT * FROM data')
    res = cur.fetchmany(10)
    con.close()
    return [dict(col) for col in res]

@app.route('/fields')
def fields():
    return get_cols()

@app.route('/search')
def search():
    cols = get_cols()
    args = {col: request.args[col] for col in cols if col in request.args}
    search_data = ' AND '.join(f'{key}="{value}"' for key, value in args.items())
    con = get_db_connection()
    print(f'SELECT * FROM data WHERE {search_data}')
    cur = con.execute(f'SELECT * FROM data WHERE {search_data}')
    res = cur.fetchall()
    con.close()
    return [dict(col) for col in res]
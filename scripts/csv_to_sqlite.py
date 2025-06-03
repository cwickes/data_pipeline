import csv
import sqlite3

con = sqlite3.connect('data/processed_data.db')
cur = con.cursor()
res = cur.execute("SELECT name FROM sqlite_master WHERE name='data'")
if res.fetchone() is None:
    with open('data/U.S._Chronic_Disease_Indicators.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        # Get first row from CSV (column names)
        cols = next(reader)

        # Build CREATE TABLE statement using column names
        create_table_statement = 'CREATE TABLE data({})'.format(', '.join(cols))
        cur.execute(create_table_statement)

        # Build INSERT INTO statement with qmark style
        insert_with_qmark = 'INSERT INTO data VALUES ({})'.format(', '.join('?' for col in cols))
        for row in reader:
            cur.execute(insert_with_qmark, row)
    con.commit()
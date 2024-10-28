import pandas as pd
from sodapy import Socrata
from dotenv import load_dotenv
import os
import psycopg2 as pg2

load_dotenv()
myapptoken = os.getenv('MYAPPTOKEN')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
client = Socrata("data.ny.gov",
                 myapptoken)

connection = pg2.connect(dbname='MTA Open Data', user=postgres_user, password=postgres_password)

#List of queries
create_table_query = """ 
CREATE TABLE IF NOT EXISTS traffic 
    (   
        id SERIAL PRIMARY KEY,
        plaza_id SMALLINT,
        date TIMESTAMP,
        hour SMALLINT,
        direction text,
        vehicles_ezpass integer,
        vehicles_vtoll integer
    )
    """
insert_query = """
    INSERT INTO traffic (plaza_id, date, hour, direction, vehicles_ezpass, vehicles_vtoll)
    VALUES
    (%s,%s,%s,%s,%s,%s)
    """

update_query = """
    UPDATE traffic 
    SET direction = CASE
        WHEN direction = 'I' THEN 'Inbound'
        WHEN direction = 'O' THEN 'Outbound'
        ELSE direction
    END
    """

total_vehicle_query = """
    ALTER TABLE traffic 
    ADD COLUMN total_vehicle integer
    """


def create_tables():
    with connection:
        curr = connection.cursor()
        curr.execute(create_table_query)
        connection.commit()
        curr.close()

def insert(data):
    with connection:
        curr = connection.cursor()
        for rows in data.itertuples():
            curr.execute(insert_query, (rows.plaza_id, rows.date, rows.hour, rows.direction, rows.vehicles_e_zpass, rows.vehicles_vtoll))
        connection.commit()
        curr.close()

def update_table():
    with connection:
        curr = connection.cursor()
        curr.execute(update_query)
        connection.commit()
        curr.close()

def alter_table():
    with connection:
        curr = connection.cursor()
        curr.execute(total_vehicle_query)
        connection.commit()
        curr.close()

def fetch_and_process():
    results = client.get("qzve-kjga", where="plaza_id = 30", limit=100000)
    results_df = pd.DataFrame.from_records(results)
    
    return results_df

def close():
    with connection:
        connection.close()
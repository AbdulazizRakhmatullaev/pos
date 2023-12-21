import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os, time, platform, json
from datetime import datetime
import pyodbc as db
from apscheduler.schedulers.blocking import BlockingScheduler


def etl():
    print("ETL function started...\n")
    #
    # iterating through files in the folder and doing some stuff.
    print("Iterating through files...")
    folder_path = Path(
        "/Users/abdu/Documents/Developer/Python/FromMAAB/tasks/pos/files"
    )

    files = folder_path.iterdir()
    fns = {}

    for paths in files:
        directory, file_full_name = os.path.split(paths)
        file_name, extension = file_full_name.rsplit(".", -1)

        if any(char.isdigit() for char in file_name):
            # If file_name contains numbers, i replace it with only alphabetic characters (customer_20231221 for example)
            only_str = "".join(char for char in file_name if char.isalpha())
            fns[only_str] = f"{directory}/{file_name}.{extension}"
        else:
            fns[file_name] = f"{directory}/{file_name}.{extension}"
    print("Iterating through files ended.\n")

    # connecting and deploying to prem SQL server
    print("Connection and deploying to SQL Server...")
    database_url = "mssql+pyodbc://sa:reallyStrongPwd123@localhost/ingest?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(database_url)

    start_date = datetime.now()
    start_time = time.time()

    status = ""
    tbs = []

    try:
        for i in fns.items():
            try:
                df = pd.read_csv(i[1])
                df.to_sql(i[0], con=engine, if_exists="replace", index=False)
                tbs.append(i[0] + "_success")
            except Exception:
                tbs.append(i[0] + "_fail")
                continue
        status = "success"
    except Exception:
        status = "fail"

    end_time = time.time()
    duration_seconds = end_time - start_time
    print("Connection and deploying to SQL Server ended.\n")

    # logging to keep track transactions
    tbs_json = json.dumps(tbs)
    print("Gathering data for logs.")
    insert_statement = text(
        f"INSERT INTO logs (host_name, status, tables, duration_seconds, start_date, end_date) "
        f"VALUES ('{platform.node()}', '{status}', '{tbs_json}', {duration_seconds}, '{start_date.strftime('%Y-%m-%d %H:%M:%S')}', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')"
    )

    # Insert data into the logs table
    with engine.connect() as connection:
        connection.execute(insert_statement)
        connection.commit()

    engine.dispose()
    print("Gathering data for logs ended.\n")
    print("ETL function stopped. Press Ctrl+C to exit.\n")


scheduler = BlockingScheduler()
scheduler.add_job(etl, "cron", hour=5, minute=0)  # 5:00 AM

# if you want to go to toilet or you are in a hurry, run right now!
etl()

# if you want daily run
try:
    print("Daily ETL execution started, waiting...\n")
    scheduler.start()
except KeyboardInterrupt:
    print("Daily ETL execution stopped.")

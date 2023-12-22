import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os, time, platform, json
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


def etl():
    # iterating through files in the folder.
    folder_env_str = os.environ.get("folder_path")
    folder_path = Path(folder_env_str)
    files = folder_path.iterdir()
    fns = {}

    # if there are files in the folder - continue
    if any(files):
        print("ETL function started...\n")
        print("Iterating through files...")

        for paths in files:
            directory, file_full_name = os.path.split(paths)
            file_name, extension = file_full_name.rsplit(".", -1)

            if any(char.isdigit() for char in file_name):
                # If file_name contains numbers, it replaces it with only alphabetic characters (customer_20231221 for example)
                only_str = "".join(char for char in file_name if char.isalpha())
                fns[only_str] = f"{directory}/{file_name}.{extension}"
            else:
                fns[file_name] = f"{directory}/{file_name}.{extension}"
        print("Iterating through files ended.\n")

        # connecting and deploying to prem SQL server
        print("Connection and deploying to SQL Server...")
        database_url = os.environ.get("database_url")
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
        print("Gathering data for logs.")
        tbs_json = json.dumps(tbs)
        insert_statement = text(
            f"INSERT INTO logs (host_name, status, tables, duration_seconds, start_date, end_date) "
            f"VALUES ('{platform.node()}', '{status}', '{tbs_json}', {duration_seconds:.2f}, '{start_date.strftime('%Y-%m-%d %H:%M:%S')}', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')"
        )

        # Insert data into the logs table
        with engine.connect() as connection:
            connection.execute(insert_statement)
            connection.commit()

        engine.dispose()
        print("Gathering data for logs ended.\n")
        print("ETL function stopped.")
    # if not, stop the code.
    else:
        print(f"There are no files in the specified folder ({folder_path})")


# - Run right now
etl()

# - Run daily
# scheduler = BlockingScheduler()
# scheduler.add_job(etl, "cron", hour=5, minute=0)  # 5:00 AM

# try:
#     print("Daily ETL execution started, waiting...\n")
#     scheduler.start()
# except KeyboardInterrupt:
#     print("Daily ETL execution stopped.")

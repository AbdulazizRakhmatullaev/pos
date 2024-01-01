This is a code written on python for your daily execution of the ETL process.

ABOUT:
- Developed a data integration process capable of dynamically adapting to different CSV files generated daily by the POS system.
- Implemented a mechanism to identify and handle new files with varying names, creating corresponding tables in the ingest database as needed.
- Implemented a scheduling mechanism to automate the daily execution of the ETL process.
- Established robust monitoring and logging mechanisms to track key performance metrics and identify potential issues.
-	Logs relevant information for auditing purposes and facilitates troubleshooting in case of failures.
- Implemented a comprehensive error-handling strategy to address potential issues during the ETL process.

WHAT TO CHANGE IN THE CODE:
- 1. You need folder path, set the variable "folder_path" 
- - for Mac/Linux
  - export folder_path="you whole folder path where files are stored"
- - For Windows
  - set folder_path="you whole folder path where files are stored"

- 2. You need connection string, set the variable "database_url"
- - For Mac/Linux
  - export database_url="mssql+pyodbc://<username>:<password>@<server-name>/<database-name>?driver=ODBC+Driver+17+for+SQL+Server"
- - For Windows (I don't use windows so I don't know what connection they use, just replace with the right one)
  - set database_url="mssql+pyodbc://<username>:<password>@<server-name>/<database-name>?driver=ODBC+Driver+17+for+SQL+Server"

And that's it, you are good to go...

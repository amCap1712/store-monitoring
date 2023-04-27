# Store Monitoring

A FastAPI webapp to analyse store uptime data and generate an uptime report for each store.

# Steps to run

1) Clone the Github repo and checkout the directory.
2) Create a virtual environment and install the required dependencies.
   ```bash
   python3 -m venv venv
   ./venv/bin/activate
   pip install -r requirements.txt
   ``` 
3) Start a Postgres container to store the data and create a .env file to allow the app to connect to it.
   ```bash
   docker run -p 5432:5432 --name store-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres
   echo 'SQLALCHEMY_DATABASE_URI="postgresql://postgres:mysecretpassword@localhost:5432/postgres"' >> .env
   ```
4) Download the input csv files and run the import cli tool.
   ```bash
   python cli.py STORE_TIMEZONE_PATH STORE_TIMINGS_PATH STORE_OBSERVATIONS_PATH
   ```
5) Run the FastAPI webapp.
   ```bash
   python -m uvicorn app.main:app
   ```
6) In another terminal, query the webapp to trigger a report.
   ```bash
   curl http://127.0.0.1:8000/trigger_report
   ```
7) Using the report id returned in the previous step, to retrieve the report. If the report is still
   running, a json string will be returned showing the pending status else the csv file for the report
   will be downloaded.
   ```bash
   curl http://127.0.0.1:8000/get_report/{report_id}
   ```

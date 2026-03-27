# Based on section 9.2 AIS Data Cleaning from the MobilityDataScience book
# and https://github.com/mahmsakr/MobilityDataScienceClass/tree/main/Mobility%20Data%20Cleaning
from sqlalchemy import create_engine, text
import json
import zipfile
import os
from utils import get_csv_from_zip
# Load database configuration from config.json
with open("../config.json", "r") as file:
    config = json.load(file)

# Construct the database URL for SQLAlchemy
database_url = (
    f"postgresql://{config['DB_USER']}:{config['DB_PASS']}@"
    f"{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"
)

# Create the SQLAlchemy engine
engine = create_engine(database_url)
data_csv_path = get_csv_from_zip("aisdk-2026-03-01.zip")


# Data collection and 9.3 Cleaning Static Attribute +9.4 Voyage related Attribbutes:
try:
    # Read the SQL file CleaningStaticAttributes.sql
    with open("CleaningStaticAttributes.sql", "r") as sql_file:
        sql_query = sql_file.read()

    # Create a text object with parameters
    stmt = text(sql_query)

    # Execute with parameter
    with engine.connect() as conn:
        # Start a transaction
        with conn.begin():
            # Execute the COPY command with parameter
            conn.execute(stmt, {"data_csv_path": data_csv_path})
            print(f"Successfully loaded data from {data_csv_path}")

except Exception as e:
    print(f"Error loading data: {e}")
    raise

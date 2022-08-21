"""
SQL Data Extract Tool

Process flow:
1. Get the tag metadata
2. Get list of historic databases available
3. Extract analog and discrete data tables to CSV
5. Save CSV as parquet file
6. Delete CSV files
"""

# import libraries
import logging
import os
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

# set up logging toboth terminal and file
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("sql_export.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

# load environment variables
site = os.getenv("SITE")
server = os.getenv("SERVER")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

# set convert options for pyarrow
convert_options = pa.csv.ConvertOptions()
convert_options.column_types = {
    "TimeStamp": pa.timestamp("ns"),
    "TagID": pa.int32(),
    "TagValue": pa.float64(),
    "Quality": pa.int32(),
}

# retrieve tag metadata
query = """
set nocount on; 
print 'TagID,TagName,Description,ChangeTimestamp,SourceUniqueTagID,Maximum,Minimum,EngUnits'; 
select [TagID],[TagName],REPLACE([Description], ',', '') AS [Description],[ChangeTimestamp],[SourceUniqueTagID],[Maximum],[Minimum],[EngUnits]
from TagManager_Tags;
"""
sqlcmd = f'sqlcmd -S {server} -d ProcessDataDB -U {username} -P {password} -Q "{query}" -s "," -h -1 -W -o "./data/{site}_metadata.csv"'
logger.info("-- Getting tag metadata...")
os.system(sqlcmd)

# read metadata and compress it
df = pd.read_csv(f"./data/{site}_metadata.csv")
df.to_csv(f"./data/{site}_metadata.csv.gz", compression="gzip", index=False)
logger.info(f"Done.\n")

# get list of history DBs
# note: check permission of the user to access 'master' database
query = "set nocount on; print 'Database';select name from sys.Databases where name like 'History%'"
sqlcmd = f'sqlcmd -S {server} -d ProcessDataDB -U {username} -P {password} -Q "{query}" -s "," -h -1 -W -o "./data/{site}_dbs.csv"'
logger.info("-- Getting ProcessDataDB History databases...")
os.system(sqlcmd)

# get list of historic databases
df = pd.read_csv(f"./data/{site}_dbs.csv")
logger.info(f"Found {len(df.index)} history DBs.\n")

list_of_dbs = list(df.Database)

# iterate through list of historic databases
for db in list_of_dbs:
    logger.info(f"--- Starting export for {db}")

    try:
        logger.info(f"--- Downloading analog table... ")
        query = f"set nocount on; print 'TimeStamp,TagID,TagValue,Quality'; select * from Historian_AnalogTagData"
        output_file = f"./data/csv/{site}_{db}_analog.csv"
        sqlcmd = (
            f'sqlcmd -S {server} -d {db} -U {username} -P {password} -Q "{query}" -s "," -h -1 -W -o "{output_file}"'
        )
        logger.info(sqlcmd)
        os.system(sqlcmd)
        logger.info("Done.")

        logger.info(f"--- Converting analog table... ")
        output_file = f"./data/csv/{site}_{db}_analog.csv"
        table = pv.read_csv(output_file, convert_options=convert_options)
        pq.write_table(
            table,
            f"./data/parquet/{site}_{db}_analog.parquet",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        logger.info("Done.")

        logger.info(f"--- Delete analog CSV file... ")
        os.remove(output_file)
        logger.info("Done.\n")
    except:
        logger.error(f"*** ERROR processing analog data for {db}! ***")

    try:
        logger.info(f"--- Downloading discrete table... ")
        query = f"set nocount on; print 'TimeStamp,TagID,TagValue,Quality'; select * from Historian_DiscreteTagData"
        output_file = f"./data/csv/{site}_{db}_discrete.csv"
        sqlcmd = (
            f'sqlcmd -S {server} -d {db} -U {username} -P {password} -Q "{query}" -s "," -h -1 -W -o "{output_file}"'
        )
        logger.info(sqlcmd)
        os.system(sqlcmd)
        logger.info("Done.")

        logger.info(f"--- Converting discrete table... ")
        output_file = f"./data/csv/{site}_{db}_discrete.csv"
        table = pv.read_csv(output_file, convert_options=convert_options)
        pq.write_table(
            table,
            f"./data/parquet/{site}_{db}_discrete.parquet",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        logger.info("Done.")

        logger.info(f"--- Delete discrete CSV file... ")
        os.remove(output_file)
        logger.info("Done.\n")
    except:
        logger.error(f"*** ERROR processing discrete data for {db}! ***\n")

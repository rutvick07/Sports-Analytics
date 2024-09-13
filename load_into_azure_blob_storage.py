from scrape import *
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import os

load_dotenv()

functions = [league_table,top_scorers,detail_top,player_table,all_time_table,all_time_winner_club,top_scorers_seasons,goals_per_season]

#functions = [league_table]

def to_blob(func):

    '''
    Converts the output of a given function to Parquet format and uploads it to Azure Blob Storage.
    This function takes a provided function, calls it to obtain data, and then converts the data into
    an Arrow Table. The Arrow Table is serialized into Parquet format and uploaded to an Azure Blob
    Storage container specified in the function. The function's name is used as the blob name.
    '''

    file_name = func.__name__
    func = func()

    # Convert DataFrame to Arrow Table
    table = pa.Table.from_pandas(func)

    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)

    connection_string = os.getenv('BLOB_STORAGE_CONN_STR')
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = os.getenv('BLOB_STORAGE_CNTR')
    blob_name = f"{file_name}.parquet"
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    try:
        blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
        print(f"{blob_name} successfully updated")
    except Exception as e:
        print(f"{e} error occured. File upload for `{blob_name}` was skipped.")


for items in functions:
    to_blob(items)
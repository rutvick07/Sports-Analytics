from scrape import *
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

functions = [league_table,top_scorers,detail_top,player_table,all_time_table,all_time_winner_club,top_scorers_seasons,goals_per_season]

username = os.getenv('PSG_USR') 
hostname = os.getenv('PSG_HOST') 
port = os.getenv('PSG_PORT') 
database_name = os.getenv('PSG_DBN') 
password = os.getenv('PSG_PSWD')

db_url = f"postgresql://{username}:{password}@{hostname}:{port}/{database_name}"

db = create_engine(db_url)
conn = db.connect() 
for fun in functions:
    function_name = fun.__name__
    result_df = fun()  # Call the function to get the DataFrame
    try:
        result_df.to_sql(function_name, con=conn, if_exists='replace', index=False)
        print(f'Pushed data for {function_name}')
    except Exception as e:
        print(f'{e} error occured. Data for `{function_name}` was not pushed.')
    

# Close the database connection
conn.close()
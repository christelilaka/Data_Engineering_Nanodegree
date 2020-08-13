import os
import json
import boto3
import requests
import pandas as pd 
import configparser
from io import StringIO
from pandas.io.json import json_normalize

# Find the path of the config file
root_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(root_dir, 'aws.cfg')
    
config = configparser.ConfigParser()
config.read(f'{file_path}')

season = config['NBA']['SEASON']

s3 = boto3.resource('s3', region_name="us-west-2", 
                        aws_access_key_id=config['AWS']['KEY'], 
                        aws_secret_access_key=config['AWS']['SECRET'])

def get_gameIDs(nba_season):
    """
    This function download a JSON file from the NBA website that has the schedule of games for a specific season, uses a dataframe to extract the columns needed then returns a list of game IDs and the schedule dataframe.
    
    Take the nba season, like 2019, as an argument.
    """
    
    url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/'+str(season)+'/league/00_full_schedule_week.json'
    schedule = requests.get(url).json()
    games = json_normalize(data=schedule['lscd'], record_path=['mscd', 'g'])
    schedule = games[['gid', 'gcode', 'gdte', 'an', 'ac', 'as']]
    
    game_ids = [games.gid.iloc[t] for t in range(games.shape[0])]
    
    return game_ids, schedule

def get_pbp(season, game_id):
    """
    This function takes a season like 2019 and an array of game IDs as arguments, uses these arguments to construct a url to download the JSON file related to the game ID. Returns a dataframe with the columns needed.
    """
    
    url_pbp = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/' + str(season) + '/scores/pbp/'+game_id+'_full_pbp.json'
    data = requests.get(url_pbp).json()
    df = json_normalize(data = data['g'], record_path=['pd', 'pla'])
    df = df[['de', 'etype', 'locX', 'locY', 'mtype', 'pid', 'tid']]
    df.insert(1, 'game_code', data['g']['gcode'], allow_duplicates=True)
    
    return df

# Dump Dataframe to CSV in S3
def pbp_toS3(bucket_name, df, s3, index):
    """
    This function takes a bucket name, a dataframe that has the play by play data of a game, a S3 object, and an index as arguments then write the dataframe in CSV format in the S3 bucket.
    """
    
    csv_buffer = StringIO()
    file_name = 'PlayByPlay/game.csv.' + str(index)
    df.to_csv(csv_buffer, index=False)
    
    s3object = s3.Object(bucket_name, file_name)
    _ = s3object.put(Body=csv_buffer.getvalue())
    
def schedule_toS3(bucket_name, df, s3):
    """
    This function takes a bucket name, a dataframe that has the schedule data of a season, a S3 object as arguments then write the dataframe in CSV format in the S3 bucket.
    """
    
    csv_buffer = StringIO()
    file_name = 'nba_schedule/schedule.csv'
    df.to_csv(csv_buffer, index=False)
    
    s3object = s3.Object(bucket_name, file_name)
    _ = s3object.put(Body=csv_buffer.getvalue())
    
    
game_IDs, schedule = get_gameIDs(nba_season=season)


for i in range(len(game_IDs)):
    try:
        game_df = get_pbp(season=season, game_id=game_IDs[i])
        pbp_toS3(bucket_name=config['S3']['S3_BUCKET'], df=game_df, s3=s3, index=i)
    except:
        continue

schedule_toS3(bucket_name=config['S3']['S3_BUCKET'], df=schedule, s3=s3)

import os
import json
import boto3
import requests
import pandas as pd 
import configparser
from io import StringIO
from pandas.io.json import json_normalize

def get_s3(aws_key, aws_secret):
    """
    Description: 
                This function creates an identifier S3 object.
    Arguments: 
                aws_key: AWS access key ID
                aws_secret: AWS secret access key
    Returns:
            A S3 object.
    """
    
    s3 = boto3.resource('s3', region_name="us-west-2", 
                        aws_access_key_id=aws_key, 
                        aws_secret_access_key=aws_secret)
    return s3

def players_toS3(bucket_name, s3, season):
    """
    This function download a JSON file from the website that has the NBA statistics; extracts the columns needed in a dataframe then write the dataframe in CSV format in a S3 bucket.
    
    Arguments:
            bucket_name: Then name of the bucket where the CSV file will be saved
            s3: S3 identifier object
            season: The starting year of a NBA season like 2019 for the 2019-2020 season
    """
    
    url = 'https://data.nba.net/10s/prod/v1/' + str(season) + '/players.json'
    players = requests.get(url).json()
    df_players = json_normalize(data=players['league'], record_path=['standard'])
    df_players = df_players[['firstName', 'lastName', 'personId', 'teamId', 'jersey', 'isActive', 'pos', 'heightFeet', 'heightInches', 'weightPounds', 'dateOfBirthUTC', 'collegeName', 'country', 'draft.teamId', 'draft.pickNum', 'draft.roundNum', 'draft.seasonYear']]
    df_players.replace(r'^\s*$', 'null', regex=True, inplace=True)
    
    csv_buffer = StringIO()
    file_name = 'Players/players_' + str(season) + '.csv'
    df_players.to_csv(csv_buffer, index=False)
    
    s3object = s3.Object(bucket_name, file_name)
    _ = s3object.put(Body=csv_buffer.getvalue())
    
    
def main():
    """
    This is the main function that create the s3 object then use the "players_toS3" function to write the players data in CSV format in the s3 bucket.
    """
    
    # Find the path of the config file
    root_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(root_dir, 'aws.cfg')
    
    config = configparser.ConfigParser()
    config.read(f'{file_path}')
    
    key =config['AWS']['KEY']
    secret =config['AWS']['SECRET']
    bucket = config['S3']['S3_BUCKET']
    nba_season = config['NBA']['SEASON']
    
    s3 = get_s3(aws_key=key, aws_secret=secret)
    
    players_toS3(bucket_name=bucket, s3=s3, season=nba_season)
    
    
if __name__ == "__main__":
    main()
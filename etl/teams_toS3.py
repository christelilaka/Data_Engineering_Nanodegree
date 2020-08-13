import os
import json
import boto3
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


def teams_toS3(s3, bucket_name, file_name):
    """
    Read a JSON file from a S3 bucket, transform the data to extract the columns needed then save the dataframe in CSV format in S3.
    
    Input:
            s3: S3 identifier object
            bucket_name: the name of the bucket where the JSON file is saved
            file_name: the name of the JSON file
    """
    
    obj_get = s3.Object(bucket_name, file_name)
    data = json.load(obj_get.get()['Body'])
    
    df_teams = json_normalize(data=data['league'], record_path=['standard'])
    df_teams = df_teams[['teamId', 'isNBAFranchise', 'city', 'fullName', 'tricode', 'nickname',  'confName', 'divName']]
    
    csv_buffer = StringIO()
    to_file = 'Teams/nba_teams.csv'
    df_teams.to_csv(csv_buffer, index=False)
    
    s3object = s3.Object(bucket_name, to_file)
    _ = s3object.put(Body=csv_buffer.getvalue())
    
def main():
    """
    This function read the JSON data from S3, transforms it then load it back in CSV format.
    1. Create a S3 object by calling the "get_s3" function
    2. Read, transform, and load the data in CSV back in S3 by calling the "teams_toS3" function.
    """
    
    # Find the path of the config file
    root_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(root_dir, 'aws.cfg')
    
    config = configparser.ConfigParser()
    config.read(f'{file_path}')
    
    key =config['AWS']['KEY']
    secret =config['AWS']['SECRET']
    bucket = config['S3']['S3_BUCKET']
    file_name = 'teams_info.json'
    
    s3 = get_s3(aws_key=key, aws_secret=secret)
    
    teams_toS3(s3=s3, bucket_name=bucket, file_name=file_name)

    
if __name__ == "__main__":
    main()
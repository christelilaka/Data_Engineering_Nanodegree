import os
import configparser
import psycopg2

# Find the path of the config file
root_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(root_dir, 'aws.cfg')
    
config = configparser.ConfigParser()
config.read(f'{file_path}')


host = config['CLUSTER']['HOST']
db = config['CLUSTER']['DB_NAME']
user = config['CLUSTER']['DB_USER']
pwd = config['CLUSTER']['DB_PASSWORD']
port = config['CLUSTER']['DB_PORT']
key = config['AWS']['KEY']
secret = config['AWS']['SECRET']

conn = psycopg2.connect(f"host={host} dbname={db} user={user} password={pwd} port={port}")
cur = conn.cursor()

# Data paths
pbp = config['S3']['PBP']
players = config['S3']['PLAYERS']
teams = config['S3']['TEAMS']
schedule = config['S3']['SCHEDULE']

staging_tables = ['staging_pbp', 'staging_players', 'staging_teams', 'staging_schedule']
s3_data = [pbp, players, teams, schedule]


# Copy data from S3 to staging tables in Redshift
for data in list(zip(staging_tables, s3_data)):
    
    query = (f"""copy {data[0]}
                from {data[1]}
                credentials 'aws_access_key_id={key};aws_secret_access_key={secret}'
                CSV
                delimiter ','
                IGNOREHEADER 1;""")
    cur.execute(query)
    conn.commit()
    
    print(f'Copy function completed for {data[0]} table')
    
conn.close()
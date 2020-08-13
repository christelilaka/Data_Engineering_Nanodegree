import os
import sys 
import psycopg2
import configparser


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

# ------ Test on PLAYERS table: Check if there's duplicate ----------------
players_test = ("""SELECT player_id, COUNT(player_id)
                FROM players
                GROUP BY player_id
                HAVING COUNT(player_id) >1;""")

cur.execute(players_test)
player_query = cur.fetchall()

if player_query == []:
    print("TEST SUCCEEDED")
    print("No duplicate found in the PLAYERS table")
else:
    sys.exit("Test FAILED because of duplicate in the PLAYERS table")
    
    
# ------ Test on TEAMS table: Check if there's duplicate ------------------
teams_test = ("""SELECT team_id, COUNT(team_id)
                FROM teams
                GROUP BY team_id
                HAVING COUNT(team_id) >1;""")

cur.execute(teams_test)
teams_query = cur.fetchall()

if teams_query == []:
    print("\nTEST SUCCEEDED")
    print("No duplicate found in the TEAMS table")
else:
    sys.exit("Test FAILED because of duplicate in the TEAMS table")
    
    
# ------ Test on NBA_SCHEDULE table: Check if there's duplicate -----------
schedule_test = ("""SELECT game_code, COUNT(game_code)
                    FROM nba_schedule
                    GROUP BY game_code
                    HAVING COUNT(game_code) > 1;""")
cur.execute(schedule_test)
schedule_query = cur.fetchall()

if schedule_query == []:
    print("\nTEST SUCCEEDED")
    print("No duplicate found in the NBA_SCHEDULE table")
else:
    sys.exit("Test FAILED because of duplicate in the NBA_SCHEDULE table")

    
# ------ Test on PLAY_BY_PLAY table: Check if there's data -----------
pbp_test = ("SELECT COUNT(*) FROM play_by_play;")
cur.execute(pbp_test)
data = cur.fetchall()[0]
pbp_query = data[0]

if pbp_query > 10:
    print("\nTEST SUCCEEDED")
    print("There is data in the PLAY_BY_PLAY table")
else:
    sys.exit("Test FAILED because no data found in the PLAY_BY_PLAY table")
    
    
conn.close()
print('\nPASS ALL TESTS')
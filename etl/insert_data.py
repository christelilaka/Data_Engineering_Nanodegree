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

# Connect to the database 
conn = psycopg2.connect(f"host={host} dbname={db} user={user} password={pwd} port={port}")
cur = conn.cursor()


insert_pbp = ("""INSERT INTO play_by_play (game_code,game_date, description, event_type, locX, locY, make_type, player_id, team_id)
            SELECT  pbp.game_code,
                    ss.game_date, 
                    pbp.description, 
                    pbp.event_type, 
                    pbp.locX, 
                    pbp.locY, 
                    pbp.make_type, 
                    pbp.player_id, 
                    pbp.team_id
            FROM staging_pbp AS pbp
            JOIN staging_schedule AS ss
            ON pbp.game_code = ss.game_code;
        """)


insert_schedule = ("""INSERT INTO nba_schedule(game_code, game_date, arena, city, state)
                        SELECT  game_code,
                                game_date,
                                arena,
                                city,
                                state
                        FROM staging_schedule;
                    """)


insert_teams = (""" INSERT INTO teams(team_id, isNBAFranchise, city, full_name, tricode, nickname, conference_name, division_name)
                    SELECT team_id, isNBAFranchise, city, full_name, tricode, nickname, conference_name, division_name
                    FROM staging_teams""")

insert_players = ("""INSERT INTO players
                    SELECT first_name, last_name, player_id, team_id, jersey, isActive,
                            position, height_feet, height_inches, weight_pound, date_of_birth,
                            college_name, country, draft_team_id, draft_pick_num, draft_round, draft_season_year
                    FROM staging_players;""")


queries = [insert_pbp, insert_schedule, insert_teams, insert_players]

# Execute each insert query in the queries list
for query in queries:
    cur.execute(query)
    conn.commit()

conn.close()
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

# Connect to the database
conn = psycopg2.connect(f"host={host} dbname={db} user={user} password={pwd} port={port}")
cur = conn.cursor()

drop_staging_pbp = ("DROP TABLE IF EXISTS staging_pbp CASCADE")
drop_staging_schedule = ("DROP TABLE IF EXISTS staging_schedule CASCADE")
drop_staging_teams = ("DROP TABLE IF EXISTS staging_teams CASCADE")
drop_staging_players = ("DROP TABLE IF EXISTS staging_players CASCADE")
drop_play_by_play = ("DROP TABLE IF EXISTS play_by_play CASCADE")
drop_nba_schedule = ("DROP TABLE IF EXISTS nba_schedule CASCADE")
drop_teams = ("DROP TABLE IF EXISTS teams CASCADE")
drop_players = ("DROP TABLE IF EXISTS players CASCADE")


staging_play_by_play = ("""
                            CREATE TABLE IF NOT EXISTS staging_pbp(
                            description varchar,
                            game_code varchar,
                            event_type varchar,
                            locX int,
                            locY int,
                            make_type varchar,
                            player_id varchar,
                            team_id varchar);
                        """)

staging_schedule = ("""
                        CREATE TABLE IF NOT EXISTS staging_schedule(
                        game_id varchar,
                        game_code varchar,
                        game_date DATE,
                        arena varchar,
                        city varchar,
                        state varchar);
                    """)

staging_teams = ("""
                    CREATE TABLE IF NOT EXISTS staging_teams(
                    team_id varchar,
                    isNBAFranchise boolean,
                    city varchar,
                    full_name varchar,
                    tricode char(3),
                    nickname varchar,
                    conference_name varchar,
                    division_name varchar);
                """)

staging_players = ("""
                        CREATE TABLE IF NOT EXISTS staging_players(
                        first_name varchar,
                        last_name varchar,
                        player_id varchar,
                        team_id varchar,
                        jersey varchar,
                        isActive varchar,
                        position varchar,
                        height_feet varchar,
                        height_inches varchar,
                        weight_pound varchar,
                        date_of_birth varchar,
                        college_name varchar,
                        country varchar,
                        draft_team_id varchar,
                        draft_pick_num varchar,
                        draft_round varchar,
                        draft_season_year varchar);
                    """)

# Create final Tables
pbp = ("""
            CREATE TABLE IF NOT EXISTS play_by_play(
            pk_id int IDENTITY(0,1) primary key not null,
            game_code varchar REFERENCES nba_schedule (game_code),
            game_date DATE sortkey distkey not null,
            description varchar,
            event_type varchar,
            locX int,
            locY int,
            make_type varchar,
            player_id varchar REFERENCES players (player_id),
            team_id varchar REFERENCES teams (team_id));
        """)

schedule = ("""
                CREATE TABLE IF NOT EXISTS nba_schedule(
                game_code varchar primary key not null,
                game_date DATE sortkey distkey,
                arena varchar,
                city varchar,
                state varchar);
            """)

teams = ("""
            CREATE TABLE IF NOT EXISTS teams(
            team_id varchar primary key not null sortkey,
            isNBAFranchise boolean,
            city varchar,
            full_name varchar,
            tricode char(3),
            nickname varchar,
            conference_name varchar,
            division_name varchar) diststyle all;
        """)

player = ("""
            CREATE TABLE IF NOT EXISTS players(
            first_name varchar,
            last_name varchar,
            player_id varchar not null primary key sortkey,
            team_id varchar REFERENCES teams (team_id),
            jersey varchar,
            isActive varchar,
            position varchar,
            height_feet varchar,
            height_inches varchar,
            weight_pound varchar,
            date_of_birth varchar,
            college_name varchar,
            country varchar,
            draft_team_id varchar,
            draft_pick_num varchar,
            draft_round varchar,
            draft_season_year varchar) diststyle all;
        """)


drop_queries = [drop_staging_pbp, drop_staging_schedule, drop_staging_teams, drop_staging_players, drop_play_by_play, drop_nba_schedule, drop_players, drop_teams]

# Drop tables if their exist
for query in drop_queries:
    cur.execute(query)
    conn.commit()


queries = [staging_play_by_play, staging_schedule, staging_teams, staging_players, schedule, teams, player, pbp]

# Create tables
for query in queries:
    cur.execute(query)
    conn.commit()

conn.close()

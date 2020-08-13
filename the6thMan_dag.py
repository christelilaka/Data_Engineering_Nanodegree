from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import BashOperator

dag = DAG('nba_etl', description='This pipeline download NBA data then load it into Redshift',
          schedule_interval='0 12 * * *',
          start_date=datetime(2020, 7, 16), catchup=False)

start_etl = DummyOperator(task_id='etl_started', dag=dag)


load_pbp_schedule_toS3 = BashOperator(
    task_id="load_pbp_schedule_toS3",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/pbp_and_schedule_toS3.py",
    dag=dag)

load_players_toS3 = BashOperator(
    task_id="load_players_toS3",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/players_toS3.py",
    dag=dag)

load_teams_toS3 = BashOperator(
    task_id="load_teams_toS3",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/teams_toS3.py",
    dag=dag)

create_tables = BashOperator(
    task_id="create_tables",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/create_tables.py",
    dag=dag)

copy_to_stagings = BashOperator(
    task_id="copy_to_stagings",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/copy_toStaging.py",
    dag=dag)

load_to_final_tables = BashOperator(
    task_id="load_to_final_tables",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/insert_data.py",
    dag=dag)

data_test = BashOperator(
    task_id="data_test",
    bash_command="python ~/airflow/dags/ilaka_dag/etl/data_tests.py",
    dag=dag)

end_etl = DummyOperator(task_id='etl_finished', dag=dag)



#ETL pipeline

start_etl >>[load_teams_toS3, load_players_toS3, load_pbp_schedule_toS3] >> create_tables >> copy_to_stagings >> load_to_final_tables >> data_test >> end_etl
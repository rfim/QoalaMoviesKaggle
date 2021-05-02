"""
### Qoala Series DAG
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.incubator.apache.org/tutorial.html)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
import os
from airflow.hooks import PostgresHook
import json
import numpy as np


def load_data(ds, **kwargs):

	dir_name = os.path.join(os.path.dirname(__file__),'src/data/archive/series/series', '')
	entries = os.listdir(dir_name)

	for entry in entries:
		pg_hook  = PostgresHook(postgres_conn_id='weather_id',schema='MovieDB')
		conn = pg_hook.get_conn()
		tot_name = os.path.join(os.path.dirname(__file__),'src/data/archive/series/series', entry)
		json_file = True
		if "json" in tot_name :
			with open(tot_name, 'r') as inputfile:
				doc = json.load(inputfile)
		else :
			json_file = False
			#doc = json.load(inputfile)
		if json_file is True:
			id_series        = int(doc['id'])
			series_name      = str(doc['original_name']).replace('\'','\'\'')
			if doc['first_air_date'] is None:
				first_air        = 0
			else :
				first_air        = int(doc['first_air_date'].split('-')[0])
			country_origin = "N/A"
			season_count     = str(doc['seasons'][0]['season_number'])
			category_1   = "N/A"
			category_2   = "N/A"
			series_rating    = float(doc['popularity'])
			avg_rating       = float(doc['vote_average'])
			total_clicks     = float(doc['vote_count'])

			valid_data  = True

			row  =  (id_series, series_name, first_air,  country_origin, season_count, category_1, category_2, series_rating,avg_rating, total_clicks)
			insert_cmd = """INSERT INTO series
                            (id_series, series_name, first_air,
                            country_origin, season_count, category_1, category_2,
                            series_rating, avg_rating, total_clicks)
                            VALUES
                            (%s, '%s', %s, '%s', %s, '%s', '%s', %s, %s, %s);""" % (id_series, series_name, first_air,  country_origin, season_count, category_1, category_2, series_rating,avg_rating, total_clicks)
			print(insert_cmd)
			if valid_data is True:
				curr = conn.cursor()
				curr.execute(insert_cmd)
				conn.commit()
				conn.close()


# Define the default dag arguments.
default_args = {
		'owner' : 'Qoala',
		'depends_on_past' :False,
		'email' :['rpa.calculation@gmail.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 5,
		'retry_delay': timedelta(minutes=1)
		}


# Define the dag, the start date and how frequently it runs.
# I chose the dag to run everday by using 1440 minutes.
dag = DAG(
		dag_id='stag_series_json_postgres',
		default_args=default_args,
		start_date=datetime(2021,5,1),
		schedule_interval=timedelta(minutes=1440))


doc1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

doc1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

# First task is to query get the weather from openweathermap.org.
task1 = BashOperator(
			task_id='get_series',
			bash_command='python ~/src/makeTable_Movie.py' ,
			dag=dag)


# Second task is to process the data and load into the database.
task2 =  PythonOperator(
			task_id='transform_load',
			provide_context=True,
			python_callable=load_data,
			dag=dag)

# Set task1 "upstream" of task2, i.e. task1 must be completed
# before task2 can be started.
doc1 >> [task1, task2]

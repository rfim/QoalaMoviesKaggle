from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from psycopg2 import connect, extensions, sql
import psycopg2



def make_database():
    """
    Make the Postgres database and create the table.
    """

    dbname = 'MovieDB'
    username = 'airflow'
    server = 'localhost'

    # declare a new PostgreSQL connection object


    engine   = create_engine('postgresql+psycopg2://%s:airflow@localhost/%s'%(username,dbname))
    if not database_exists(engine.url):
        create_database(engine.url)

    conn = psycopg2.connect(database = dbname, user = username, host=server, password='airflow')
    curr = conn.cursor()

    create_table ="""CREATE TABLE IF NOT EXISTS movies
                (
                    id_movie            TEXT,
                    movie_name          TEXT,
                    year                REAL,
                    country_origin      REAL,
                    category_1          DATE,
                    category_2          REAL,
                    movie_rating        REAL,
                    avg_rating          REAL,
                    total_clicks        REAL
                )
                ;

                CREATE TABLE IF NOT EXISTS series
                (
                    id_series            TEXT,
                    series_name          TEXT,
                    first_air            REAL,
                    season_count         REAL,
                    country_origin       DATE,
                    category_1           DATE,
                    category_2           REAL,
                    series_rating        REAL,
                    avg_rating           REAL,
                    total_clicks         REAL
                )
                ;

                """


    curr.execute(create_table)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    make_database()

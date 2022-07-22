# handle dataframes
import pandas as pd
# handle DB
import sqlalchemy

# custom modules
import get_env_variables
import prepare_dataset

"""This module creates the 'fail_predict' database on PostreSQG using sqlalchemy

The 'fail_predict' database contains 3 tables :
    users : handles user authentification to access to streamlit application 
        (log in / sign in)
        columns : userID, username, password
        This table is empty when DB is created
    dataset : holds 2018 accounts of french companies used to build a predictive model on their
        failure probability the year after (2019)
    naf : INSEE mapping table used to handle different granularity levels 
        for sectorial analysis
"""


def add_fk(table_from, column_name, table_to):
    """This function executes an SQL statement to the DB 
        This statement creates a Foreign Key from a given column

    Args:
        table_from (string): table holding the field as a primary key
        column_name (string): column name (field) to be used as a foreign key
        table_to (string): table holding the foreign key 
    """
    engine.execute(f"""ALTER TABLE {table_to} ADD CONSTRAINT {column_name}_fk 
        FOREIGN KEY ({column_name}) REFERENCES {table_from} ({column_name});""")


def add_pk(table_name, column_name):
    """This function executes an SQL statement to the DB 
        This statement creates a Primary Key from a given column

    Args:
        table_name (string): table holding the field as a primary key
        column_name (string): column name (field) to be used as a primary key
    """    
    engine.execute(f"""ALTER TABLE {table_name} ADD CONSTRAINT {column_name}_pk 
        PRIMARY KEY ({column_name});""")

def clear_db():
    """This function executes an SQL statement to the DB 
        This statement deletes all the tables of the 'public' schema 
        by dropping the schema 'public' and then creating a new one
    """    
    engine.execute("DROP SCHEMA public CASCADE;")
    engine.execute("CREATE SCHEMA public;")


def create_dataset_table():
    """
    This function creates in DB the table named 'dataset' and fills it with relevant data
    """    
    dataset = pd.read_csv(f"{prepare_dataset.data_path}/total_2018.csv")
    dataset = dataset.drop_duplicates()
    dataset = prepare_dataset.add_custom_cols(dataset)
    dataset = prepare_dataset.rename_cols(dataset)
    dtypes_dict = prepare_dataset.get_dataset_dtype_dict(dataset)
    dataset.to_sql('dataset', engine, if_exists='replace', index=False, dtype = dtypes_dict)
    add_pk('dataset', 'siret')
    add_fk('naf', 'code_ape', 'dataset')


def create_db():
    """
    This function creates in DB following tables :
        users (empty)
        naf (filled with relevant data)
        dataset (filled with relevant data)
    """
    create_users_table()
    create_naf_table()
    create_dataset_table()


def create_naf_table():
    """
    This function creates in DB the table named 'naf' and fills it with relevant data
    """
    naf = pd.read_excel(f"{prepare_dataset.data_path}/table_NAF2-NA.xls", header=[1])
    naf = prepare_dataset.prepare_naf_df(naf)
    naf = prepare_dataset.rename_cols(naf)
    dtypes_dict = prepare_dataset.get_dataset_dtype_dict(naf)
    naf.to_sql('naf', engine, dtype=dtypes_dict)
    add_pk('naf', 'code_ape')

def create_users_table():
    """
    This function creates in DB the empty table named 'users'
    """
    engine.execute(f"""CREATE TABLE IF NOT EXISTS users
        (userid SERIAL PRIMARY KEY,
        username VARCHAR,
        password VARCHAR);
        """)

def drop_table(table_name):
    """This function executes an SQL statement on the database
        This statement drops a given table

    Args:
        table_name (string): table to drop
    """
    engine.execute(f"DROP TABLE IF EXISTS {table_name};")
   
def reset_db():
    """This function resets the database :
        Delete all tables of 'public' schema
        Then recreate all tables (naf, users, dataset) 
        and fills the naf and dataset tables (users table remains empty)
    """
    clear_db()
    create_db()

### MAIN ###
if __name__ == '__main__':
    engine = sqlalchemy.create_engine(get_env_variables.DATABASE_URI, echo=True)
    reset_db()




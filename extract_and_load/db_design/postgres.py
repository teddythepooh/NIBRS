import sqlalchemy
import psycopg2
import polars as pl
from io import StringIO
from .raw_tables import Base

# https://www.psycopg.org/docs/cursor.html
# https://www.psycopg.org/docs/connection.html

# https://stackoverflow.com/questions/77160257/postgresql-create-database-cannot-run-inside-a-transaction-block

class Postgres:
    def __init__(self, credentials: dict, schemas: list) -> None:
        '''
        credentials: a dictionary of key:value pairs where keys are host, dbname, user, and port
        schemas: a list of desired schemas
                
        dbname nor the schemas need not exist beforehand
        '''
        self.credentials = credentials
        self.schemas = schemas

    def _create_psycopg2_connection(self, db_name: str = None) -> psycopg2.extensions.connection:
        '''
        db_name: if specified, the db_name in config["credentials"]["db_name"] will be overridden
        '''
        try:
            if not db_name:
                connection = psycopg2.connect(**self.credentials)
            else:
                connection = psycopg2.connect(
                    dbname = db_name,
                    **{k: v for k, v in self.credentials.items() if k != "dbname"}
                    )

            return connection
        except TypeError:
            raise TypeError("Instance has invalid credentials: it must be a key:value pair "
                            "with all necessary arguments to form psycopg2 connection.")

    def _build_sqlalchemy_url(self) -> sqlalchemy.URL:
        credentials = self.credentials
        url = sqlalchemy.URL.create("postgresql+psycopg2",
                                    username = credentials["user"],
                                    port = credentials["port"],
                                    host = credentials["host"],
                                    database = credentials["dbname"])
        
        return url
    
    def create_sqlalchemy_engine(self) -> sqlalchemy.Engine:
        engine = sqlalchemy.create_engine(self._build_sqlalchemy_url())
        
        return engine
        
    def initialize_database(self, default_db: str = "postgres") -> None:
        '''
        default_db: an existing db in postgresql, defaults to 'postgres' if none specified
        
        creates a db called self.credentials["dbname"] by connecting to default_db; checking 
        for the existence of self.credentials["dbname"]; creates it if necessary; then establishing 
        a new connection for schema creation. Unlike the subsequent methods below, this uses a psycopg2 connection 
        rather than SQLAlchemy. This implementation is possible using a SQLAlchemy engine, but I'm too lazy to 
        make the switch. I was originally playing around with psycopg2, before I realized that many polars/pandas 
        methods expect a SQLAlchemy engine for connectivity with a db.
        '''
        try:
            credentials = self.credentials
            schemas = self.schemas
            
            # CREATE DATABASE
            connection = self._create_psycopg2_connection(db_name = default_db)
            connection.autocommit = True
            
            with connection.cursor() as cur:
                db_name = credentials["dbname"]
                cur.execute(f"select pg_database.datname from pg_database where pg_database.datname = '{db_name}'")
                if not cur.fetchone():
                    cur.execute(f"create database {db_name}")
                else:
                    print(f"Database '{db_name}' already exists.")

            # CREATE SCHEMAS   
            nibrs_db_connection = self._create_psycopg2_connection()
            nibrs_db_connection.autocommit = True
            
            with nibrs_db_connection as con:
                with con.cursor() as cur:
                    command = "\n".join([f"create schema if not exists {schema};" for schema in schemas]) 
                    cur.execute(command)
                    
            print(f"{', '.join(schemas)} schemas successfully created.")
        except psycopg2.OperationalError:
            raise psycopg2.OperationalError(
                f"Default db {default_db} not found: it is needed to establish connection and create db {db_name}."
                )
        finally:
            connection.close()
            nibrs_db_connection.close()
    
    @staticmethod
    def construct_copy_sql_code(table_name: str, columns: list) -> str:
        cols = "(" + ",".join(columns) + ")"
        
        return f"COPY {table_name} {cols} FROM STDIN with CSV HEADER"
    
    def ingest_raw_table_into_sql(self, table_to_ingest: pl.DataFrame, sql_table: str, raw_table_metadata: Base) -> None:
        '''
        table_to_ingest: polars dataframe
        sql_table: name of sql_table, including schema (e.g., raw.table_name)
        raw_table_metadata: declarative base containing raw tables' metadata
        
        writes out table_to_ingest to file-like object as .csv, then ingests the file into sql_table through COPY using
        a psycopg2 connection
        '''
        if list(table_to_ingest.columns) == list(raw_table_metadata.metadata.tables[sql_table].columns.keys()):
            csv_buffer = StringIO()
            
            table_to_ingest.write_csv(csv_buffer)
            
            csv_buffer.seek(0)
            
            with self._create_psycopg2_connection() as con:
                with con.cursor() as cur:
                    cur.copy_expert(sql = Postgres.construct_copy_sql_code(table_name = sql_table, columns = table_to_ingest.columns), 
                                    file = csv_buffer)
            
            con.close()
            cur.close()
        else:
            raise Exception("Mismatched columns.")

import sqlalchemy
import psycopg2
import polars as pl
from datetime import date
from io import StringIO

from .raw_tables import Base
from . import metadata_table

# https://www.psycopg.org/docs/cursor.html
# https://www.psycopg.org/docs/connection.html

# https://stackoverflow.com/questions/77160257/postgresql-create-database-cannot-run-inside-a-transaction-block

class Postgres:
    def __init__(self, credentials: dict, schemas: list) -> None:
        '''
        credentials: A dictionary where keys are host, dbname, user, and port.
        schemas: A list of desired schemas.
        '''
        self.credentials = credentials
        self.schemas = schemas

    def _create_psycopg2_connection(self, db_name: str = None) -> psycopg2.extensions.connection:
        '''
        db_name: If specified, the db_name in config["credentials"]["db_name"] will be overridden.
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
        default_db: An existing db in postgresql, defaulting to 'postgres' if none specified.
        
        Creates a db called self.credentials["dbname"] by connecting to default_db; checking for the existence of 
        self.credentials["dbname"]; creates it if necessary; then establishing a new connection for schema creation. 
        Unlike the subsequent methods below, this uses a psycopg2 connection make the switch. I was originally playing 
        around with psycopg2, before I realized that many polars/pandas methods expect a SQLAlchemy engine.
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

    def _is_table_ingested(self, table_name: str) -> bool:
        '''
        Check if table_name has already been ingested.
        '''
        engine = self.create_sqlalchemy_engine()
        
        with sqlalchemy.orm.Session(engine) as session:
            existing = session.query(metadata_table.IngestedFiles).filter_by(table = table_name).first()
            return existing is not None

    def _record_ingestion(self, table_name: str) -> None:
        engine = self.create_sqlalchemy_engine()
        
        with sqlalchemy.orm.Session(engine) as session:
            new_record = metadata_table.IngestedFiles(
                table = table_name,
                ingestion_date = date.today()
            )
            session.add(new_record)
            session.commit()
    
    def ingest_table_into_db(self, table_to_ingest: pl.DataFrame, db_table: str, table_base: Base, source_file: str) -> None:
        '''
        table_to_ingest: Polars dataframe.
        db_table: Name of table in db, inclusive of schema (e.g., raw.arrests).
        table_base: SQLAlchemy Base containing table metadata.
        source_file: Name of file from which table_to_ingest originates for metadata tracking (e.g., arrests_2022.parquet).
        
        Ingests table_to_ingest into db_table. If table_to_ingest was already ingested, it will be skipped.
        '''
        if self._is_table_ingested(source_file):
            print(f"{source_file} was already ingested. Skipping...")
            return None
        
        if list(table_to_ingest.columns) == list(table_base.metadata.tables[db_table].columns.keys()):
            csv_buffer = StringIO()
            table_to_ingest.write_csv(csv_buffer)
            csv_buffer.seek(0)
            
            with self._create_psycopg2_connection() as con:
                with con.cursor() as cur:
                    cur.copy_expert(
                        sql = Postgres.construct_copy_sql_code(table_name = db_table, columns = table_to_ingest.columns), 
                        file = csv_buffer
                    )
            
            con.close()
            cur.close()
            
            self._record_ingestion(source_file)
            print(f"Successfully ingested '{source_file}' into '{db_table}.'")
            
        else:
            raise Exception("Mismatched columns.")

import sqlalchemy
import psycopg2
import polars as pl

from datetime import date
from io import StringIO

from . import raw_tables
from . import metadata_table

# https://www.psycopg.org/docs/cursor.html
# https://www.psycopg.org/docs/connection.html
# https://docs.sqlalchemy.org/en/20/tutorial/data_select.html
# https://docs.sqlalchemy.org/en/20/tutorial/data_insert.html
# https://stackoverflow.com/questions/77160257/postgresql-create-database-cannot-run-inside-a-transaction-block

class Postgres:
    raw_schema = raw_tables
    metadata_schema = metadata_table
    
    def __init__(self, credentials: dict, schemas: list):
        '''
        credentials: A dictionary with host, dbname, user, and port keys.
        schemas: A list of desired schemas. At minimum, it must have "raw" and "metadata."
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
        default_db: An existing db in postgres, defaulting to 'postgres' if none specified.
        
        Creates a db called self.credentials["dbname"], if none exists, by connecting to default_db then 
        establishing a new connection for schema creation.
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

    def _is_file_ingested(self, source_file: str) -> bool:
        '''
        source_file: File name to check in ingested_files table from metadata schema.
        
        Checks if source_file has already been ingested into db.
        '''
        engine = self.create_sqlalchemy_engine()
        
        metadata = Postgres.metadata_schema.IngestedFiles
        
        stmt = sqlalchemy.select(metadata).where(metadata.table == source_file)
        
        with sqlalchemy.orm.Session(engine) as session:
            return bool(session.execute(stmt).first())

    def _record_ingestion(self, source_file: str) -> None:
        '''
        source_file: File name to record in ingested_files table from metadata schema.
        
        Records source_file in ingested_files table along with today's date.
        '''
        engine = self.create_sqlalchemy_engine()
        
        metadata = Postgres.metadata_schema.IngestedFiles
        
        stmt = sqlalchemy.insert(metadata).values(table = source_file, ingestion_date = date.today())
        
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
    
    def ingest_table_into_db(self, table_to_ingest: pl.DataFrame, db_table: str, source_file: str) -> None:
        '''
        table_to_ingest: Polars dataframe to ingest.
        db_table: Name of table in db, inclusive of schema (e.g., raw.arrests).
        source_file: Name of file from which table_to_ingest originates for metadata tracking (e.g., arrests_2022.parquet).
        
        Ingests table_to_ingest into db_table. If table_to_ingest was already ingested, it will be skipped.
        '''
        if self._is_file_ingested(source_file):
            print(f"{source_file} was already ingested. Skipping...")
            return None
        
        db_table_columns = Postgres.raw_schema.Base.metadata.tables[db_table].columns.keys()
        
        if list(table_to_ingest.columns) == list(db_table_columns):
            csv_buffer = StringIO()
            table_to_ingest.write_csv(csv_buffer)
            csv_buffer.seek(0)
            
            with self._create_psycopg2_connection() as con:
                with con.cursor() as cur:
                    cur.copy_expert(
                        sql = Postgres.construct_copy_sql_code(
                            table_name = db_table, 
                            columns = table_to_ingest.columns
                            ), 
                        file = csv_buffer
                    )
            
            con.close()
            cur.close()
            
            self._record_ingestion(source_file)
            print(f"Successfully ingested '{source_file}' into '{db_table}.'")
            
        else:
            raise Exception("Mismatched columns.")

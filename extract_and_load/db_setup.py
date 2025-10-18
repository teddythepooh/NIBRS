import argparse

from sqlalchemy import Engine
from types import ModuleType

from core import general
from db_design import Postgres

def create_tables(table_base: ModuleType, 
                  sqlalchemy_engine: Engine, 
                  postgres_config: dict) -> None:
    """
    table_base: SQLAlchemy module that configures tables and corresponding metadata.
    sqlalchemy_engine: A SQLAlchemy engine with proper credentials.
    postgres_config: A dictionary with a schemas key.
    
    Creates tables
    """
    schema = table_base.Base.metadata.schema
    
    if schema in postgres_config["schemas"]:
        table_base.Base.metadata.create_all(bind = sqlalchemy_engine)
    else:
        raise Exception(f"'{schema}' schema not found in postgres_config['schemas'].")

def main(config_file: dict):
    '''
    Creates database and schemas based on config_file["postgresql"]. Finally, creates tables in raw and metadata schemas.
    '''
    postgres_config = config_file["postgresql"]
    
    print("Created database, schemas, and tables...")
    db_config = Postgres(credentials = postgres_config["credentials"], schemas = postgres_config["schemas"])
    db_config.initialize_database()

    sqlalchemy_engine = db_config.create_sqlalchemy_engine()
    create_tables(Postgres.raw_schema, sqlalchemy_engine, postgres_config)
    create_tables(Postgres.metadata_schema, sqlalchemy_engine, postgres_config)
    
    print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help = ".yml file with postgresql key, under which exists credentials and schemas keys")
    args = parser.parse_args()
    
    main(config_file = general.load_yaml(args.f))

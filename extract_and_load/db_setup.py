from core import general
from db_design import Postgres, raw_tables, metadata_table

from sqlalchemy import Engine
from sqlalchemy.orm import DeclarativeBase

def create_tables(table_base: DeclarativeBase, 
                  sqlalchemy_engine: Engine, 
                  postgres_config: dict):
    """
    Creates table(s) in database based on metadata defined in table_base.
    """
    schema = table_base.Base.metadata.schema
    
    if schema in postgres_config["schemas"]:
        table_base.Base.metadata.create_all(bind = sqlalchemy_engine)
    else:
        raise Exception(f"'{schema}' schema not found in postgres_config['schemas'].")

def main():
    '''
    Creates database and schemas based on config_file["postgresql"]. Finally, creates tables in raw and metadata schemas.
    '''
    config_file = general.load_yaml("./configuration/config.yml")
    postgres_config = config_file["postgresql"]
    
    print("Created database, schemas, and tables...")
    db_config = Postgres(credentials = postgres_config["credentials"], schemas = postgres_config["schemas"])
    db_config.initialize_database()

    sqlalchemy_engine = db_config.create_sqlalchemy_engine()
    create_tables(metadata_table, sqlalchemy_engine, postgres_config)
    create_tables(raw_tables, sqlalchemy_engine, postgres_config)
    
    print("Done.")

if __name__ == "__main__":
    main()

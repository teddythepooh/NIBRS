import argparse
from core import general
from db_design import Postgres, raw_tables
from pathlib import Path

def main(config_file: dict) -> None:
    '''
    config_file: Nested dictionary with a top-level key "postgresql." which should look like below (as a .yml file).
    -----------
    postgresql:
      credentials:
        host: localhost
        dbname: nibrs
        user: postgres
        port: 5432
      schemas:
        - raw
        - cleaned
        - crosswalks
    ----------

    Creates a db called config_file["postgresql"]["credentials"]["db_name"], if one doesn't exist, 
    along with schemas in config_file["postgresql"]["schemas"]. Finally, creates tables in the raw 
    schema based on metadata defined in ./src/db_design/raw_tables.py.
    '''
    postgres_config = config_file["postgresql"]
    
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok = True)
    logging = general.create_logger(log_file = logs_dir.joinpath(f"{Path(__file__).stem}.log"))
    
    logging.info("Creating database and schemas...")
    try:
        db_config = Postgres(credentials = postgres_config["credentials"], 
                             schemas = postgres_config["schemas"])
        db_config.initialize_database()
    except KeyError:
        raise KeyError("config_file must have 'credentials' and 'schemas' keys.")
    
    logging.info("Creating tables in raw schema...")
    if raw_tables.Base.metadata.schema in postgres_config["schemas"]:
        sqlalchemy_engine = db_config.create_sqlalchemy_engine()
        raw_tables.Base.metadata.create_all(bind = sqlalchemy_engine)
    else:
        raise Exception((f"Make sure that '{raw_tables.Base.metadata.schema}' schema "
                         "is defined in config_file['postgresql']['schemas']."))

    logging.info("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser() #description = main.__doc__
    parser.add_argument("-c", help = "path to .yml file with db config")
    
    args = parser.parse_args()
    
    config = general.load_yaml(args.c)
    
    main(config_file = config)

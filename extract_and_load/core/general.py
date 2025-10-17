from pathlib import Path
import logging
import yaml

def create_logger(log_file: Path, name: str = "LOGS", level: int = logging.DEBUG) -> logging.Logger:
    log_file = Path(log_file)
    logger = logging.getLogger(name)
    logger.setLevel(level)  
    file_handler = logging.FileHandler(log_file, mode = "w")
    console_handler = logging.StreamHandler()

    # https://docs.python.org/3/library/logging.html#logrecord-attributes
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def load_yaml(file: Path) -> dict:
    try:
        with open(file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"{file} not found.")

def create_output_dir(output_dir_str: str, log_file: str) -> tuple:
    output_dir = Path(output_dir_str)
    logs_dir = output_dir.joinpath("logs")
    
    logs_dir.mkdir(parents = True, exist_ok = True)
    
    if not log_file.endswith(".log"):
        raise Exception("Please make sure that the log file name ends with .log for readability.")
    
    logger = create_logger(log_file = logs_dir.joinpath(log_file))
    
    return output_dir, logger

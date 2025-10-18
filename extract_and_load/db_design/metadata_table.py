from sqlalchemy import MetaData, String, Date
from sqlalchemy.orm import DeclarativeBase, mapped_column

ingestion_tracker = MetaData(schema = "metadata")

class Base(DeclarativeBase):
    metadata = ingestion_tracker

class IngestedFiles(Base):
    __tablename__ = "ingested_files"
    
    table = mapped_column(String, primary_key = True)
    ingestion_date = mapped_column(Date)

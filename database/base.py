from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base


# Declarative base
Base = declarative_base()

# Database engine
engine = create_engine("sqlite:///congressgov.db")

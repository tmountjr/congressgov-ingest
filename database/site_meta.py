"""Maintain and insert data for `site_meta` table."""

from datetime import datetime
import pytz
from sqlalchemy import Column, DateTime, String, inspect
from sqlalchemy.orm import Session
from database.base import Base, BaseOrm


class SiteMeta(Base):
    """Site Metadata table."""

    __tablename__ = "site_meta"
    __table_args__ = {"schema": "site_meta"}

    last_update = Column(DateTime, primary_key=True)
    tz = Column(String(500), nullable=False)


class SiteMetaOrm(BaseOrm):
    """ORM class to interact with the site_meta table."""

    def __init__(self, data_dir="./"):
        super().__init__(data_dir)

    def drop_all_tables(self):
        """Override to restrict dropping tables."""
        raise NotImplementedError("This operation is not allowed in subclasses.")

    def create_table(self):
        """Create the site_meta table."""
        if not inspect(self.engine).has_table(
            SiteMeta.__tablename__, schema=SiteMeta.__table_args__["schema"]
        ):
            SiteMeta.__table__.create(self.engine)

    def drop_table(self):
        """Drop the site_meta table."""
        if inspect(self.engine).has_table(
            SiteMeta.__tablename__, schema=SiteMeta.__table_args__["schema"]
        ):
            SiteMeta.__table__.drop(self.engine)

    def set_last_update(self):
        """Insert a new update record."""
        with Session(self.engine) as session:
            update = SiteMeta(
                last_update=datetime.now(), tz="America/New_York"
            )
            session.add(update)
            session.commit()

# Science Product Table
# Schema:
#   science_product_id: int (primary key)
#   instrument_configuration_id: int (foreign key)
#   mode: str
#   reference_timestamp: datetime

from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

import swxsoc

from . import base_table as Base
from . import science_file_table as ScienceFileTable

_current_class: Any = None


def _build_class() -> Any:
    mission_name = swxsoc.config["mission"]["mission_name"]

    class ScienceProductTable(Base.Base):  # type: ignore
        __tablename__ = f"{mission_name}_science_product"

        # ID Of Science Product (Primary Key)
        science_product_id = Column(Integer, primary_key=True, autoincrement=True)

        # ID Of Instrument Configuration (Foreign Key)
        instrument_configuration_id = Column(
            Integer,
            ForeignKey(
                f"{mission_name}_instrument_configuration.instrument_configuration_id"
            ),
        )

        # Mode Of Science Product
        mode = Column(String)

        # Reference Timestamp Of Science Product
        reference_timestamp = Column(DateTime)

        children = relationship(
            lambda: ScienceFileTable.return_class(),
            back_populates="parent",
            cascade="all, delete",
        )

        def __init__(
            self,
            instrument_configuration_id: int,
            mode: str,
            reference_timestamp: datetime,
        ) -> None:
            """
            Constructor for Science Product Table
            """
            self.instrument_configuration_id = instrument_configuration_id  # type: ignore[assignment]
            self.mode = mode  # type: ignore[assignment]
            self.reference_timestamp = reference_timestamp  # type: ignore[assignment]

        def __repr__(self) -> str:
            return super().__repr__()  # type: ignore[no-any-return]

    return ScienceProductTable


def reconfigure() -> Any:
    """
    Rebuild the ORM class for the currently active mission.
    """
    global _current_class
    _current_class = _build_class()
    return _current_class


def return_class() -> Any:
    """
    Return Class
    """
    if _current_class is None:
        reconfigure()
    return _current_class

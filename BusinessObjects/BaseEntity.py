from typing import Optional
from sqlalchemy import Column, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class BaseEntity:
    created_by = Column(String(255), nullable=False)
    created_on = Column(String(255), nullable=False)
    modified_by = Column(String(255), nullable=False)
    modified_on = Column(String(255), nullable=False)
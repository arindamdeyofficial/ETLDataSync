#region imports

from sqlite3 import Timestamp
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, func
from BusinessObjects.BaseEntity import BaseEntity
from Repository.SqlAlchemySetupPostgres import SqlAlchemySetupPostgres

#endregion imports

#region Model def: start
class ProductDto(BaseEntity, SqlAlchemySetupPostgres.Base):
    __tablename__ = "product"

    id = Column(String(255), primary_key=True)
    desc = Column(String(255))
    brand_id = Column(String(255))
    brand = Column(String(255))
    l10_code = Column(String(255))
    l20_code = Column(String(255))    
    stores = Column(Integer)
    supplier = Column(String(255))
    supplier_id = Column(String(255)) 
    image_url = Column(String(255))
    is_deleted = Column(Boolean, default=False)
    sales = Column(Boolean, default=False)
    stores_in_scan_as_you_shop = Column(Boolean, default=False)
    stores_is_dot_com = Column(Boolean, default=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now())
    
#endregion Model def: End

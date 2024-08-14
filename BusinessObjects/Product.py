#region imports

from datetime import datetime
from pydantic import BaseModel, EmailStr, Field, ValidationError

#endregion imports

#region Model def: start

class Product(BaseModel):
    id: str = Field(str)
    desc: str = Field(str)
    brand_id: str = Field(str)
    brand: str = Field(str)
    l10_code: str = Field(str)
    l10_desc: str = Field(str)
    l20_code: str = Field(str)
    l20_desc: str = Field(str)
    l30_code: str = Field(str)
    l30_desc: str = Field(str)
    l40_code: str = Field(str)
    l40_desc: str = Field(str)
    L50Code: str = Field(str)
    L50Description: str = Field(str)
    SupplierId: str = Field(str)
    Supplier: str = Field(str)
    SalesDataAvailable: str = Field(str)
    Stores: int = Field(int)
    ImageUrl: str = Field(str)
    IsDeleted: bool = Field(bool)
    StoresInScanAsYouShop: bool = Field(bool)
    StoresIsDotCom: bool = Field(bool)
    CreatedAt: datetime = Field(datetime)
    UpdatedAt: datetime = Field(datetime)
        
#endregion Model def: End

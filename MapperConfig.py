from BusinessObjects.dbModels import BookDto, ReviewDto, UserDto
from automapper import mapper
from BusinessObjects.ProductDto import *
from BusinessObjects.models import *
from BusinessObjects.Product import Product

class MapperConfig:
    #region Automapper: start
    mapper.add(Book, BookDto)
    mapper.add(BookDto, Book)
    mapper.add(Review, ReviewDto)
    mapper.add(ReviewDto, Review)
    mapper.add(User, UserDto)
    mapper.add(UserDto, User)
    #endregion Automapper: end
    #region Automapper: start
    mapper.add(Product, ProductDto, fields_mapping=
               {
                   "id": "Product.id",
                   "Description": "Product.Id",
                   "BrandId": "Product.Id",
                   "BrandDescription": "Product.Id",
                   "CategoryL10Code": "Product.Id",
                   "CategoryL20Code": "Product.Id",
                   "L10Code": "Product.Id",
                   "L10Description": "Product.Id",
                   "L20Code": "Product.Id",
                   "L20Description": "Product.Id",
                   "L30Code": "Product.Id",
                   "L30Description": "Product.Id",
                   "L40Code": "Product.Id",                   
                   "L40Description": "Product.Id",
                   "L50Code": "Product.Id",
                   "L50Description": "Product.Id",
                   "SupplierId": "Product.Id",
                   "Supplier": "Product.Id",
                   "SalesDataAvailable": "Product.Id",
                   "Stores": "Product.Id",
                   "ImageUrl": "Product.Id",
                   "IsDeletedField": "Product.Id",
                   "StoresInScanAsYouShopField": "Product.Id",
                   "StoresIsDotComField": "Product.Id"
                })
    mapper.add(ProductDto, Product, fields_mapping=
               {
                   "id": "ProductDto.id",
                   "Description": "ProductDto.Description",
                   "BrandId": "ProductDto.BrandId",
                   "BrandDescription": "ProductDto.BrandDescription",
                   "CategoryL10Code": "ProductDto.CategoryL10Code",
                   "CategoryL20Code": "ProductDto.CategoryL20Code",
                   "L10Code": "ProductDto.L10Code",
                   "L10Description": "ProductDto.L10Description",
                   "L20Code": "ProductDto.L20Code",
                   "L20Description": "ProductDto.L20Description",
                   "L30Code": "ProductDto.L30Code",
                   "L30Description": "ProductDto.L30Description",
                   "L40Code": "ProductDto.L40Code",                   
                   "L40Description": "ProductDto.L40Description",
                   "L50Code": "ProductDto.L50Code",
                   "L50Description": "ProductDto.L50Description",
                   "SupplierId": "ProductDto.SupplierId",
                   "Supplier": "ProductDto.Supplier",
                   "SalesDataAvailable": "ProductDto.SalesDataAvailable",
                   "Stores": "ProductDto.Stores",
                   "ImageUrl": "ProductDto.ImageUrl",
                   "IsDeletedField": "ProductDto.IsDeletedField",
                   "StoresInScanAsYouShopField": "ProductDto.StoresInScanAsYouShopField",
                   "StoresIsDotComField": "ProductDto.StoresIsDotComField"
                })
    #endregion Automapper: end

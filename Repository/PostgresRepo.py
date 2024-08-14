import datetime
from sqlite3 import IntegrityError, OperationalError
from typing import Union
from fastapi import Body, Depends, HTTPException, Path, status
from BusinessObjects.dbModels import BookDto
import spacy
from sqlalchemy import Date, cast, func, update
from sqlalchemy.future import select
from Helper.CommonHelper import CommonHelper
from sqlalchemy.orm import joinedload
from Helper.logHelper import LogHelper
from MapperConfig import *
from Helper.PdfHelper import PdfHelper
from Repository.SqlAlchemySetupPostgres import SqlAlchemySetupPostgres


class PostgresRepo:
    def __init__(self, logHelper: LogHelper = Depends()):
        self.logger = logHelper.logger
    #region book
    async def create_book(self, book: Book):  
            async with SqlAlchemySetupPostgres.async_session_maker() as db:        
                try:
                    bdto = mapper.to(BookDto).map(book)
                    async with db.begin():
                        db.add(bdto)
                    await db.commit()
                    await db.refresh(bdto)
                    self.generateBookSummary(bdto)
                    return book
                except IntegrityError as e:
                    # Handle potential database constraint violations
                    await db.rollback()
                    raise ValueError(f"Error creating book: {e}") from e

    async def get_all_books(self, lastsyncDate: datetime = datetime.date(2000, 1, 1)):    
            async with SqlAlchemySetupPostgres.async_session_maker() as db:  
                try:
                    books = await db.execute(select(BookDto).where(cast(BookDto.created_on, Date) >= lastsyncDate)) 
                    return books.scalars().all() 
                except Exception as e: 
                    return {"error": str(e)}

    async def get_book(self, book_id: int = Path(..., gt=0)):
        async with SqlAlchemySetupPostgres.async_session_maker() as session:
            try:
                result = await session.execute(select(BookDto).filter(BookDto.id == book_id))
                book = result.scalars().first()
                if not book:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book with ID {book_id} not found")
                return book
            except Exception as e:  # Handle broader exceptions
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    async def update_book(self, book_id: int = Path(..., gt=0), updated_data: Book = Body(...)):
        async with SqlAlchemySetupPostgres.async_session_maker() as db:
            try:
                #session = scoped_session(db)
                result = await db.execute(select(BookDto).filter(BookDto.id == book_id))
                book = result.scalars().first()
                if not book:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book with ID {book_id} not found")
                book = mapper.to(BookDto).map(updated_data)
                await db.execute(update(BookDto).where(BookDto.id == book_id).values(CommonHelper.to_dict(book)))

                await db.commit()
                #await db.refresh(book)
                return book
            except Exception as e:  # Handle broader exceptions
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

    async def delete_book(self, book_id: int = Path(..., gt=0)):
        async with SqlAlchemySetupPostgres.async_session_maker() as db:
            try:  
                reviewresult = await db.execute(select(ReviewDto).filter(ReviewDto.book_id == book_id))
                reviews = reviewresult.scalars().all()
                if reviews:
                    for review in reviews:
                        await db.delete(review)  
                await db.commit()
                result = await db.execute(select(BookDto).filter(BookDto.id == book_id))
                book = result.scalars().first()
                if not book:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Book with ID {book_id} not found")        
                await db.delete(book)
                await db.commit()
                return {"message": "Book deleted successfully"}
            
            except IntegrityError as e:  
                await db.rollback()
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
            except OperationalError as e:  
                await db.rollback()
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
            except Exception as e:  
                await db.rollback()
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    #endregion book
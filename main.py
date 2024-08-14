#region imports
import asyncio
from Repository.SqlAlchemySetupPostgres import SqlAlchemySetupPostgres
from apiapp import fastapiapp
from Controllers import EtlController

#endregion imports
app = fastapiapp
async def main():
    sqlalchemy_setup = SqlAlchemySetupPostgres()
    await sqlalchemy_setup.create_async_tables()

if __name__ == "__main__":
    asyncio.run(main())
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8020, reload=True)

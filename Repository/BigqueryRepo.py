from fastapi import Body, Depends, HTTPException, Path, status
from Helper.logHelper import LogHelper
from MapperConfig import *
from Repository.SetupBigQuery import SetupBigQuery
from Helper.BgHelper import BgHelper


class BigqueryRepo:
    def __init__(self, logHelper: LogHelper = Depends(), bgHelper: BgHelper = Depends()):
        self.logger = logHelper.logger
        self.bgHelper = bgHelper

    #region Set crud
    async def readBigData(self, project: str, dataset: str, table: str, startcount: int, endcount: int):
        """data in from Bigquery"""
        async with SetupBigQuery.getBigQueryInstance() as bgClient:
            try:
                table_ref = bgClient.get_table(f"{project}.{dataset}.{table}")
                query = f"""SELECT * FROM `{project}.{dataset}.{table}` LIMIT {((endcount + 1) - startcount)} OFFSET {startcount}"""
                query_job = bgClient.query(query)
                results = query_job.result()  
                column_names = [field.name for field in table_ref.schema]              
                return results, column_names
            except Exception as e:
                self.logger.error(f"Error readBigData: {e}", exc_info=True)
                raise
            
    async def bigDataRecordCount(self, project: str, dataset: str, table: str):
        """data count in from Bigquery"""
        async with SetupBigQuery.getBigQueryInstance() as bgClient:
            try:
                table_ref = bgClient.get_table(f"{project}.{dataset}.{table}")
                query = f"""SELECT count(*) FROM `{project}.{dataset}.{table}`"""
                query_job = bgClient.query(query)
                results = query_job.result()                
                for row in results:
                    return row[0]
            except Exception as e:
                self.logger.error(f"Error bigDataRecordCount: {e}", exc_info=True)
                raise
    
    async def bgToFile(self, project: str, dataset: str, table: str):
        """dBigquery to file save"""
        async with SetupBigQuery.getBigQueryInstance() as bgClient:
            try:
                table_ref = bgClient.get_table(f"{project}.{dataset}.{table}")
                query = f"""SELECT * FROM `{project}.{dataset}.{table}`"""
                query_job = bgClient.query(query)
                results = query_job.result()   
                schema = table_ref.schema
                column_names = [field.name for field in schema]   
                await self.bgHelper.bigqueryToFile(','.join([field.name for field in schema]) + '\n')          
                for row in results:                    
                    await self.bgHelper.bigqueryToFile(','.join(str(value) for value in row) + '\n')
            except Exception as e:
                self.logger.error(f"Error bgToFile: {e}", exc_info=True)
                raise

    async def fileToBgFormat(self):
        """file to Bigquery format"""
        try:
            return await self.bgHelper.fileToBigquery()                    
        except Exception as e:
            self.logger.error(f"Error fileToBgFormat: {e}", exc_info=True)
            raise

    #endregion Set crud

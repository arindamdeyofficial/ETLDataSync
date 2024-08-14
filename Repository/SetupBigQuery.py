from contextlib import asynccontextmanager
from fastapi import HTTPException, status
from google.cloud import bigquery

class SetupBigQuery:
    #region redis orm setup 
    @staticmethod
    @asynccontextmanager
    async def getBigQueryInstance():
        bq_client = bigquery.Client.from_service_account_json('./credential/dev/key.json')
        #bq_client = bigquery.Client()
        try:
            yield bq_client 
        except Exception as e:
            print(str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        finally:
            bq_client.close()
    #endregion redis orm setup
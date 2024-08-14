from contextlib import asynccontextmanager
from fastapi import HTTPException, status
import redis

class SetupRedis:
    #region redis orm setup 
    @staticmethod
    @asynccontextmanager
    async def getRedisInstance():
        r = redis.Redis(host='127.0.0.1', port=6379, db=0) 
        try:
            yield r  
        except Exception as e:
            print(str(e))
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        finally:
            r.close()
    #endregion redis orm setup

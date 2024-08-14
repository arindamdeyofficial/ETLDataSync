

from fastapi import Depends
from Helper.logHelper import LogHelper
import csv


class BgHelper:
    def __init__(self, filname: str ="bgdata.csv", logHelper: LogHelper = Depends()):
        self.filname = filname
        self.logger = logHelper.logger

    async def bigqueryToFile(self, data):
        try:
            with open(self.filname, 'a') as f:
                f.write(data)
        except Exception as e:
            self.logger.error(f"Error bigqueryToFile: {e}", exc_info=True)
            raise
    
    async def fileToBigquery(self):
        try:
            with open(self.filname, 'r') as f:
                return list(csv.reader(f))
        except Exception as e:
            self.logger.error(f"Error FileToBigquery: {e}", exc_info=True)
            raise

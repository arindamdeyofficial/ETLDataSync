import datetime
import json
from typing import Union
from BusinessObjects.Product import Product
from BusinessObjects.models import Book, Review
from fastapi import Depends, FastAPI, status
from Helper.BgHelper import BgHelper
from Helper.logHelper import LogHelper
from Helper.CommonHelper import CommonHelper
from Repository.RedisRepo import RedisRepo
from Repository.PostgresRepo import PostgresRepo
from Repository.BigqueryRepo import BigqueryRepo
from apiapp import fastapiapp
import time
import cProfile

app = fastapiapp
@app.get("/prodSetPostRedis")
async def syncProductsSetPostgresToRedis(postgresRepo: PostgresRepo = Depends(), logHelper: LogHelper = Depends(), redisRepo: RedisRepo = Depends()):
    """Retrieves a list of all Products in the postgres and inserts, update, delete into redis db.

    Returns:
        redis containing all Book objects in the database.
    """
    try:
        #print("hjgvhj")
        logHelper.logger.info("syncProductsSetPostgresToRedis: Start")
        lastsyncDate = datetime.datetime.now()
        books = await postgresRepo.get_all_books()
        product_data_for_redis = [CommonHelper.to_dict(book) for book in books]  # Example processing
        await redisRepo.bulk_set_value(
            {f"product:{book.id}": json.dumps(data) for book, data in zip(books, product_data_for_redis)}
        )
        rdKeys = await redisRepo.key_count_all()
        return f"Redis: {rdKeys} and Postgres: {len(books)}"
    except Exception as e:
        logHelper.logger.error(f"Error syncProductsSetPostgresToRedis: {e}", exc_info=True)
       
@app.get("/bigquerytoredis")
async def syncProductsBigqueryToRedis(bgRepo: BigqueryRepo = Depends(), logHelper: LogHelper = Depends(), redisRepo: RedisRepo = Depends(), bgHelper: BgHelper = Depends()):
    """Retrieves a list of all Products in the bigquery and inserts, update, delete into redis db.

    Returns:
        redis containing all product objects in the database.
    """
    try:
        #profiler = cProfile.Profile()
        #profiler.enable()
        msgs = ''
        rcBgQuery = await bgRepo.bigDataRecordCount("projectname", "dataset", "tablename")
        rcBgQuery = 6
        rcAtaTime = 3
        key_value_pairs = []
        prodIdsBigquery = []
        tenants = await getTenant(bgRepo, logHelper, redisRepo) 
        for tenant in tenants:   
            bg_start_time = time.time()
            keyprefix = tenant+":products:"
            for n in range(0, (rcBgQuery-1), rcAtaTime):                
                data, columns = await bgRepo.readBigData("projectname", "dataset", "tablename", n, (n+rcAtaTime-1))                
                for i, row in enumerate(data, start=0):                    
                    prodJson = json.dumps(dict(zip(columns, row)))
                    convertedJsonStr = await convertProductModel(prodJson, logHelper)
                    key = f"{keyprefix}{row[0]}"
                    key_value_pairs.append({'key': key, 'value': convertedJsonStr})
                    prodIdsBigquery.append(row[0])
                res = await redisRepo.bulk_set_json(key_value_pairs)
            nonMatchingkeys, rcRedis = await mismatchRedisandbg(prodIdsBigquery, keyprefix, bgRepo, logHelper, redisRepo)
            await updateDeleted(prodIdsBigquery, keyprefix, bgRepo, logHelper, redisRepo)
            bg_end_time = time.time()
            elapsed_time_dt = bg_end_time - bg_start_time
            msg = (f"BigQuery record Count: {rcBgQuery} \n Redis Record Count: {rcRedis} for tenant: {tenant}\n Time taken dataporting: {elapsed_time_dt}\n %s" %("" if len(nonMatchingkeys) == 0 else f"{len(nonMatchingkeys)} number of records not procesed: {nonMatchingkeys}"))
            logHelper.logger.info(msg)
            logHelper.logger.info(prodIdsBigquery)
            print(len(prodIdsBigquery))
            msgs += msg + "\n"                
        return msgs
    except Exception as e:
        print(e)
        logHelper.logger.error(f"Error syncProductsSetPostgresToRedis: {e}", exc_info=True)
    
@app.get("/bigquerytoFile")
async def syncBigquerytoFile(bgRepo: BigqueryRepo = Depends(), logHelper: LogHelper = Depends(), redisRepo: RedisRepo = Depends()):
    """Retrieves a list of all Products in the bigquery and saves into file.

    Returns:
        issuccess
    """
    try:
        await bgRepo.bgToFile("projectname", "dataset", "tablename")
    except Exception as e:
        logHelper.logger.error(f"Error syncProductsSetPostgresToRedis: {e}", exc_info=True)

@app.get("/fileToRedis")
async def syncFileToRedis(bgRepo: BigqueryRepo = Depends(), logHelper: LogHelper = Depends(), redisRepo: RedisRepo = Depends()):
    """Retrieves a list of all Products in the bigquery from file and saves into redis.

    Returns:
        issuccess
    """
    try:
        msgs = ''
        rcAtaTime = 3000
        key_value_pairs = []
        prodIdsBigquery = []   
        tenants = await getTenant(bgRepo, logHelper, redisRepo) 
        
        for tenant in tenants:   
            bg_start_time = time.time()
            keyprefix = tenant+":products:"
            data = await bgRepo.fileToBgFormat()
            rcBgQuery = len(data)
            for n in range(0, (rcBgQuery-1), rcAtaTime):
                await bgToRedisDataConvert(data, prodIdsBigquery, key_value_pairs, keyprefix, rcAtaTime, n, logHelper)
                res = await redisRepo.bulk_set_json(key_value_pairs)
            nonMatchingkeys, rcRedis = await mismatchRedisandbg(prodIdsBigquery, keyprefix, bgRepo, logHelper, redisRepo)
            await updateDeleted(prodIdsBigquery, keyprefix, bgRepo, logHelper, redisRepo)
            bg_end_time = time.time()
            elapsed_time_dt = bg_end_time - bg_start_time
            msg = (f"BigQuery record Count: {rcBgQuery} \n Redis Record Count: {rcRedis} for tenant: {tenant}\n Time taken dataporting: {elapsed_time_dt}\n %s" %("" if len(nonMatchingkeys) == 0 else f"{len(nonMatchingkeys)} number of records not procesed: {nonMatchingkeys}"))
            logHelper.logger.info(msg)
            msgs += msg + "\n"
        
        return msgs
    except Exception as e:
        logHelper.logger.error(f"Error fileToRedis: {e}", exc_info=True)

async def bgToRedisDataConvert(data, prodIdsBigquery: list, key_value_pairs: list, keyprefix: str, rcAtaTime: int, n: int, logHelper: LogHelper):
    """Convert bigquery into redis  data format.

    Returns:
        multiple
    """
    try: 
        columns = data[0]
        for i, row in enumerate(data[n:(n+rcAtaTime)], start=0):
            if n==0 and i==0:
                continue
            prodJson = json.dumps(dict(zip(columns, row)))
            convertedJsonStr = await convertProductModel(prodJson, logHelper)
            key = f"{keyprefix}{row[0]}"
            key_value_pairs.append({'key': key, 'value': convertedJsonStr})
            prodIdsBigquery.append(row[0])
    except Exception as e:
        logHelper.logger.error(f"Error mismatchRedis and bg: {e}", exc_info=True)

@app.get("/updateDeleted")
async def updateDeleted(prodIdsBigquery: list, keyprefix: str ,bgRepo: BigqueryRepo, logHelper: LogHelper, redisRepo: RedisRepo):
    """update deleted products in redis

    Returns:
        None
    """
    try:
        deletedProdIds, rcdeletedProdIds = await deletedProds(prodIdsBigquery, keyprefix, bgRepo, logHelper, redisRepo)
        deletedKeyValuePair = []
        for id in deletedProdIds:
            deletedKeyValuePair.append({'key': id, 'field': 'IsDeleted', 'value': True})
        if len(deletedKeyValuePair)>0:
            await redisRepo.set_json_field_bulk(deletedKeyValuePair)
            logHelper.logger.info(f'These Prod Ids are deleted: {deletedKeyValuePair}')
    except Exception as e:
        logHelper.logger.error(f"Error updateDeleted: {e}", exc_info=True)

@app.get("/deletedProds")
async def deletedProds(prodIdsBigquery: list, keyprefix: str ,bgRepo: BigqueryRepo, logHelper: LogHelper, redisRepo: RedisRepo):
    """Retrieves a list of all Products in the bigquery are deleted

    Returns:
        list of prodids not present in BigQuery
    """
    try:
        keys, rcRedis = await redisRepo.key_count_pattern(f"{keyprefix}*")
        redisKeys = [CommonHelper.decode_prod_id(prodId).replace(keyprefix, '') for prodId in keys]
        nonmatchingKeys = [(keyprefix+prodId) for prodId in redisKeys if prodId not in prodIdsBigquery]
        if len(nonmatchingKeys)>0:
            logHelper.logger.error(f"Below products are deleted from source BigQuery: {nonmatchingKeys}")
        return nonmatchingKeys, rcRedis
    except Exception as e:
        logHelper.logger.error(f"Error deletedProds: {e}", exc_info=True)

async def mismatchRedisandbg(prodIdsBigquery: list, keyprefix: str, bgRepo: BigqueryRepo, logHelper: LogHelper, redisRepo: RedisRepo):
    """Retrieves a list of all Products in the bigquery are not inserted into redis.

    Returns:
        list of prodids not present in Redis
    """
    try: 
        keys, rcRedis = await redisRepo.key_count_pattern(f"{keyprefix}*")
        redisKeys = [CommonHelper.decode_prod_id(prodId).replace(keyprefix, '') for prodId in keys]
        nonmatchingKeys = [prodId for prodId in prodIdsBigquery if prodId not in redisKeys] 
        if len(nonmatchingKeys)>0:
            logHelper.logger.error(f"Below products are not inserted from source to destination: {nonmatchingKeys}")
        return nonmatchingKeys, rcRedis
    except Exception as e:
        logHelper.logger.error(f"Error mismatchRedisandbg: {e}", exc_info=True)

async def getTenant(bgRepo: BigqueryRepo, logHelper: LogHelper, redisRepo: RedisRepo):
    """Retrieves a list of all tenants in the redis.

    Returns:
        list of tenants in Redis
    """
    try: 
        keys = await redisRepo.get_hash_withPattern(f"tenant:*")
        schema_names = []
        for key in keys:
            schema_name = await redisRepo.get_hash_field(key, "SchemaName")
            if schema_name != None:
                schema_names.append(CommonHelper.decode_prod_id(schema_name))
        return schema_names
    except Exception as e:
        logHelper.logger.error(f"Error getTenant: {e}", exc_info=True)

async def convertProductModel(bgProdStr: str, logHelper: LogHelper):
    """Retrieves a list of all tenants in the redis.

    Returns:
        list of tenants in Redis
    """
    try: 
        bgProd = json.loads(bgProdStr)
        prod = Product(
                id= '' if bgProd["prod_code"] == None else bgProd["prod_code"],
                desc= '' if bgProd["prod_desc"] == None else bgProd["prod_desc"],
                brand_id = '' if bgProd["brand_code"] == None else bgProd["brand_code"],
                brand = '' if bgProd["brand_name"] == None else bgProd["brand_name"],
                l10_code = '' if bgProd["prod_hier_l10_code"] == None else bgProd["prod_hier_l10_code"],
                l10_desc = '' if bgProd["prod_hier_l10_desc"] == None else bgProd["prod_hier_l10_desc"],
                l20_code = '' if bgProd["prod_hier_l20_code"] == None else bgProd["prod_hier_l20_code"],
                l20_desc = '' if bgProd["prod_hier_l20_desc"] == None else bgProd["prod_hier_l20_desc"],
                l30_code = '' if bgProd["prod_hier_l30_code"] == None else bgProd["prod_hier_l30_code"],
                l30_desc = '' if bgProd["prod_hier_l30_desc"] == None else bgProd["prod_hier_l30_desc"],
                l40_code = '' if bgProd["prod_hier_l40_code"] == None else bgProd["prod_hier_l40_code"],
                l40_desc = '' if bgProd["prod_hier_l40_desc"] == None else bgProd["prod_hier_l40_desc"],
                L50Code = '' if bgProd["prod_hier_l50_code"] == None else bgProd["prod_hier_l50_code"],
                L50Description = '' if bgProd["prod_hier_l50_desc"] == None else bgProd["prod_hier_l50_desc"],
                SupplierId = '' if bgProd["supplier_code"] == None else bgProd["supplier_code"],
                Supplier = '' if bgProd["supplier_name"] == None else bgProd["supplier_name"],
                ImageUrl = '' if bgProd["image_url"] == None else bgProd["image_url"],
                IsDeleted=False,
                SalesDataAvailable='',
                Stores=1,
                StoresInScanAsYouShop=False,
                StoresIsDotCom=False,
                CreatedAt=datetime.datetime.now(),
                UpdatedAt=datetime.datetime.now()
            )
        prod_dict = dict(prod)
        prod_dict['CreatedAt'] = prod_dict['CreatedAt'].isoformat()
        prod_dict['UpdatedAt'] = prod_dict['UpdatedAt'].isoformat()
        return json.dumps(prod_dict)

    except Exception as e:
        logHelper.logger.error(f"Error convertProductModel: {e}", exc_info=True)



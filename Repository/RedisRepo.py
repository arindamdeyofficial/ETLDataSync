import array
import json
from fastapi import Body, Depends, HTTPException, Path, status
from Helper.logHelper import LogHelper
from MapperConfig import *
from Repository import SetupRedis

class RedisRepo:
    def __init__(self, logHelper: LogHelper = Depends()):
        self.logger = logHelper.logger
    
    #endregion keyvalue crud
    async def set_json(self, key, value):
        """Creates a json with given members."""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            try:
                redis.json().set(key, '.', value)
            except Exception as e:
                # Handle exceptions appropriately
                print(f"Error creating set: {e}")
                raise

    async def set_json_field(self, key, value, field):
        """Creates a json with given members field"""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            try:
                redis.json().set(key, f"$.{field}", value)
            except Exception as e:
                # Handle exceptions appropriately
                print(f"Error set_json_field: {e}")
                raise

    async def set_json_field_bulk(self, updates):
        """updates a json with given members in bulk"""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            try:
                pipeline = redis.pipeline()
                for obj in updates:
                    key = obj["key"]
                    field = obj["field"]
                    value = obj["value"]
                    pipeline.json().set(key, f"$.{field}", value)
            except Exception as e:
                # Handle exceptions appropriately
                print(f"Error set_json_field_bulk: {e}")
                raise

    async def get_json(self, key):
        """Creates a json with given members."""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            try:
                return redis.json().get(key)
            except Exception as e:
                # Handle exceptions appropriately
                print(f"Error creating set: {e}")
                raise

    async def bulk_set_json(self, data):
        """
        Bulk sets JSON data to Redis using pipelining.

        Args:
            r: A Redis connection object.
            data: A list of tuples, where each tuple contains the key and JSON data.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            try:
                pipe = redis.pipeline()
                for item in data:
                    key = item['key']
                    value = json.loads(item['value'])
                    pipe.json().set(key, '.', value)
                pipe.execute()
                return True
            except Exception as e:
                # Handle exceptions appropriately
                print(f"Error creating set: {e}")
                raise

    #endregion keyvalue crud
    
    #region Set crud
    async def create_set(self, key, members):
        """Creates a set with given members."""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            try:
                redis.sadd(key, *members)
            except Exception as e:
                # Handle exceptions appropriately
                print(f"Error creating set: {e}")
                raise

    async def read_set(self, key):
        """Reads all members of a set."""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            members = redis.smembers(key)
            return members

    async def update_set(self, key, members_to_add, members_to_remove):
        """Updates a set by adding and removing members."""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.sadd(key, *members_to_add)
            redis.srem(key, *members_to_remove)

    async def delete_set(self, key):
        """Deletes a set."""
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.delete(key)
    #endregion Set crud

    #region hash crud
    async def create_hash(self, key, data):
        """Creates a hash with given key and data.

        Args:
            key: The hash key.
            data: A dictionary containing field-value pairs.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.hmset(key, data)

    async def get_hash(self, key):
        """Gets all fields and values of a hash.

        Args:
            key: The hash key.

        Returns:
            A dictionary containing field-value pairs.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.hgetall(key)
        
    async def get_hash_withPattern(self, keypattern):
        """Gets all fields and values of a hash.

        Args:
            key: The hash key.

        Returns:
            A dictionary containing field-value pairs.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.keys(keypattern)

    async def set_hash_field(self, key, field, value):
        """Sets a field in a hash.

        Args:
            key: The hash key.
            field: The field name.
            value: The field value.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.hset(key, field, value)

    async def get_hash_field(self, key, field):
        """Gets a field from a hash.

        Args:
            key: The hash key.
            field: The field name.

        Returns:
            The field value.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.hget(key, field)

    async def delete_hash(self, key):
        """Deletes a hash.

        Args:
            key: The hash key.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.delete(key)

    async def delete_hash_field(self, key, field):
        """Deletes a field from a hash.

        Args:
            key: The hash key.
            field: The field name.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.hdel(key, field)

    #endregion hash crud

    #region keyvalue crud
    import redis

    async def key_count_all(self):
        """gets key count in Redis
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            count = redis.dbsize()
            return count
        
    async def key_count_pattern(self, pattern: str):
        """gets key count in Redis
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            cursor = 0
            count = 0
            keys = []
            while True:
                cursor, rkeys = redis.scan(cursor=cursor, match=pattern, count=10000)
                count += len(rkeys)
                keys.extend(rkeys)
                if cursor == 0:
                    break
            return keys, count

    async def is_key_Exists(self, key):
        """check a key-value pair in Redis.

        Args:
            key: The key to store the value under
        """
        try:
            async with SetupRedis.SetupRedis.getRedisInstance() as redis:
                return redis.exists(key)
        except Exception as e:
            # Handle exceptions appropriately
            print(f"Error creating set: {e}")
            raise

    async def bulk_set_value(self, key_value_pairs):
        """Sets a key-value pair in Redis.

        Args:
            key: The key to store the value under.
            value: The value to store.
        """
        try:
            self.logger.info("bulk_set_value")
            async with SetupRedis.SetupRedis.getRedisInstance() as redis:
                redis.mset(key_value_pairs)
        except Exception as e:
            # Handle exceptions appropriately
            self.logger.error(f"Error bulk_set_value: {e}")
            raise

    async def set_value(self, key, value):
        """Sets a key-value pair in Redis.

        Args:
            key: The key to store the value under.
            value: The value to store.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.set(key, value)

    async def get_value(self, key):
        """Retrieves a value from Redis.

        Args:
            key: The key to retrieve the value for.

        Returns:
            The value associated with the key, or None if the key doesn't exist.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.get(key)

    async def delete_key(self, key):
        """Deletes a key-value pair from Redis.

        Args:
            key: The key to delete.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.delete(key)

    #endregion keyvalue crud    

    #region sortedset crud
    async def add_to_sorted_set(self, key, member, score):
        """Adds a member to a sorted set with a given score.

        Args:
            key: The name of the sorted set.
            member: The member to add.
            score: The score associated with the member.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.zadd(key, {member: score})

    async def get_sorted_set_members(self, key, start=0, end=-1):
        """Retrieves members of a sorted set.

        Args:
            key: The name of the sorted set.
            start: The start index.
            end: The end index.

        Returns:
            A list of members.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.zrange(key, start, end, withscores=True)

    async def get_member_score(self, key, member):
        """Retrieves the score of a member in a sorted set.

        Args:
            key: The name of the sorted set.
            member: The member to get the score for.

        Returns:
            The score of the member.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.zscore(key, member)

    async def remove_member(self, key, member):
        """Removes a member from a sorted set.

        Args:
            key: The name of the sorted set.
            member: The member to remove.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.zrem(key, member)

    #endregion sortedset crud

    #region lists crud
    import redis

    async def push_to_list(self, key, value):
        """Pushes a value to the right of a list.

        Args:
            key: The name of the list.
            value: The value to push.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.rpush(key, value)

    async def get_list(self, key, start, end):
        """Retrieves a range of elements from a list.

        Args:
            key: The name of the list.
            start: The start index.
            end: The end index.

        Returns:
            A list of elements.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.lrange(key, start, end)

    async def pop_from_list(self, key):
        """Removes and returns the last element of a list.

        Args:
            key: The name of the list.

        Returns:
            The removed element.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            return redis.rpop(key)

    async def delete_list(self, key):
        """Deletes a list.

        Args:
            key: The name of the list.
        """
        async with SetupRedis.SetupRedis.getRedisInstance() as redis:
            redis.delete(key)

    #endregion lists crud



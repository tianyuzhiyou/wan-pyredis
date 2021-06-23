# -*- coding: utf-8 -*-

import json
from functools import cached_property, lru_cache
import asyncio
from contextlib import asynccontextmanager
from typing import Any, List
import aioredis

class AsyncRedisCache:
    
    def __init__(self, params: dict = None):
        self.params = params
        self.redis_db = None

    @cached_property
    async def common_redis(self) -> aioredis.ConnectionsPool:
        pool = await aioredis.create_redis_pool(self.params)
        return pool

    async def init_redis_cache(self, params: dict = None, cache: aioredis.ConnectionsPool = None):
        """初始化连接池"""
        if cache:
            self.redis_db = cache
        else:
            self.params = params
            self.redis_db = await self.common_redis

    async def close_redis_cache(self):
        """关闭连接池"""
        self.redis_db.close()
        await self.redis_db.wait_closed()
        

class RedisCacheProxy(object):
    """
    该对象为缓存代理对象，封装常用的缓存使用
    """

    def __init__(self, cache=None):
        self.redis_db = cache

    @asynccontextmanager
    async def with_cache(self,
                         key: str,
                         ex: int = 30,
                         is_json: bool = False,
                         is_update: bool = True,
                         is_except: bool = False) -> Any:
        """
        @desc 将SimpleCache封装成上下文管理器，便于使用
        :param key: 缓存key
        :param ex: 缓存失效时间，单位s
        :param is_json: 是否将值转化成json格式进行存储， 默认否
        :param is_update: 是否在缓存穿透后自动加载缓存， 默认是
        :param is_except: 是否发生错误时抛出异常，默认否
        :return:
        for example:
        async with CacheProxy().with_cache("name") as cache:
            if cache.data:
                return cache.data
            data = "do something"
            cache.data = data
            return cache.data
        """
        s_cache = self.simple_cache(key, ex=ex, is_json=is_json, is_update=is_update, is_except=is_except)
        try:
            s_cache.data = await s_cache.get()
            if s_cache.data:
                s_cache.update = False
            yield s_cache
        finally:
            if s_cache.update and s_cache.data:
                await s_cache.set()

    def simple_cache(self,
                     key: str,
                     ex: int,
                     is_json: bool = False,
                     is_update: bool = True,
                     is_except: bool = False) -> Any:
        return AsyncSimpleCache(key, ex=ex, cache=self.redis_db, is_json=is_json, is_update=is_update,
                                is_except=is_except)

    @asynccontextmanager
    async def with_lock(self, key, expire=30, step=0.03):
        """
        @desc redis分布式锁封装
        :param key: 缓存key
        :param expire: 锁失效时间
        :param step: 每次尝试获取锁的间隔
        :return:
        for example:

        with RedisCacheProxy().with_lock("key_name") as lock:
            "do something"
        """
        try:
            t = await self.acquire_lock(key, expire, step)
            yield t
        finally:
            await self.release_lock(key)

    async def check_lock(self, key):
        """
        检查当前KEY是否有锁
        """
        key = 'lock:%s' % key
        status = await self.redis_db.get(key)
        if status:
            return True
        else:
            return False

    async def acquire_lock(self, key, expire=30, step=0.03):
        """
        为当前KEY加锁, 默认30秒自动解锁
        """
        key = 'lock:%s' % key
        while 1:
            get_stored = await self.redis_db.get(key)
            if get_stored:
                await asyncio.sleep(step)
            else:
                lock = await self.redis_db.setnx(key, 1)
                if lock:
                    await self.redis_db.expire(key, expire)
                    return True

    async def release_lock(self, key):
        """
        释放当前KEY的锁
        """
        key = 'lock:%s' % key
        await self.safe_delete(key)

    async def get(self, key: str) -> str:
        """字符串缓存获取"""
        data = await self.redis_db.get(key)
        return data.decode("utf-8") if data else None

    async def set(self, key: str, value: str, ex: int = 10) -> bool:
        """设置字符串"""
        data = await self.redis_db.set(key, value, expire=ex)
        return data

    async def get_many(self, keys: list) -> list:
        """
        @desc 批量获取字符串
        :params keys: [chan1, char2]
        """
        data = await self.redis_db.mget(*keys, encoding="utf-8")
        return data

    async def set_many(self, data: dict):
        """批量设置字符串缓存"""
        data = await self.redis_db.mset(data)
        return data

    async def get_data(self, key: str) -> str:
        """获取字符串数据并尝试转换json"""
        value = await self.redis_db.get(key)
        if value:
            try:
                value = json.loads(value.decode("utf-8"))
            except:
                pass
        return value

    async def set_data(self, key: str, value, ex: int=None):
        """尝试转正json字符串存储"""
        try:
            value = json.dumps(value)
        except:
            pass
        return self.redis_db.set(key, value, ex=ex)

    async def delete(self, key):
        """直接删除一个key"""
        await self.redis_db.delete(key)

    async def safe_delete(self, key: str):
        """失效一个key"""
        await self.redis_db.expire(key, -1)

    async def delete_many(self, keys: list) -> None:
        """批量key失效"""
        await self.redis_db.delete(*keys)

    @property
    def client(self):
        return self.redis_db

    async def exists(self, key: str) -> bool:
        """查询key是否存在"""
        data = await self.redis_db.exists(key)
        return data

    def hget(self, key: str, field: str):
        """获取hash类型一个键值"""
        return self.redis_db.hget(key, field)

    def hmget(self, key: str, fields: list):
        """
        批量获取hash类型键值
        :param key:
        :param fields:
        :return:
        """
        return self.redis_db.hmget(key, fields)

    async def hget_data(self, key: str, field: str) -> Any:
        """获取hash的单个key"""
        data = await self.redis_db.hget(key, field)
        return json.loads(data) if data else None

    async def hmget_data(self, key: str, fields: list) -> list:
        """
        @desc hash类型获取缓存返回一个list
        """
        data = await self.redis_db.hmget(key, *fields)
        return [json.loads(i) if i is not None else None for i in data]

    async def hmget2dict_data(self, key: str, fields: list) -> dict:
        """
        @desc hash类型获取缓存返回一个dict,尝试转换json格式
        """
        cache_list = await self.redis_db.hmget(key, fields)
        return dict(zip(fields, [json.loads(i) if i is not None else None for i in cache_list]))

    async def hgetall(self, key: str) -> str or None:
        """原生获取hash所有键值对"""
        data = await self.redis_db.hgetall(key)
        return data

    async def get_json(self, key: str) -> dict:
        """
        @desc　获取ｊｓｏｎ格式的字典数据
        """
        data = await self.redis_db.hgetall(key)
        if data:
            return {k: json.loads(v) for k, v in dict(data).items()}
        return {}

    async def set_json(self, key: str, value: dict, ex: int = None):
        """
        @desc 使用ｈａｓｈ结构存储ｊｓｏｎ数据
        :return:
        """
        cache_data = []
        for k, v in value.items():
            cache_data.extend([k, json.dumps(v)])
        if not cache_data:
            return True
        pipe = self.redis_db.pipeline()
        pipe.hmset(key, *cache_data)
        if ex:
            pipe.expire(key, int(ex))
        res = await pipe.execute()
        return res

    async def sadd(self, key: str, values: list) -> int:
        """添加元素"""
        if not values:
            return 0
        count = await self.redis_db.sadd(key, *values)
        return count

    async def spop(self, key: str, count: int = None) -> list:
        """从集合弹出元素"""
        count = 1 if not count else count
        values = await self.redis_db.spop(key, count=count)
        return values if values else []

    async def smembers(self, key: str) -> list:
        """返回一个集合所有元素"""
        values = await self.redis_db.smembers(key)
        return values if values else []

    async def scard(self, key: str) -> int:
        """获取一个集合的元素个数"""
        count = await self.redis_db.scard(key)
        return count


class AsyncSimpleCache(object):

    def __init__(self,
                 key: str,
                 ex: int = 30,
                 cache: Any = None,
                 is_json: bool = False,
                 is_update: bool = True,
                 is_except: bool = True):
        """只针对字符串和json的缓存异步上下文管理器"""
        self._key = key
        self._expire = ex
        self._client = cache
        self.data = None
        self.json = is_json
        self.update = is_update
        self._except = is_except

    async def get(self) -> str or dict:
        """获取数据"""
        if self.json:
            data = await self._client.hgetall(self._key)
            if data:
                return {key: json.loads(val) for key, val in dict(data).items()}
            return {}
        else:
            res = await self._client.get(self._key)
            return json.loads(res.decode("utf-8")) if res else None

    async def set(self) -> bool:
        """保存数据"""
        if self.json:
            assert isinstance(self.data, dict)
            cache_data = {k: json.dumps(v) for k, v in self.data.items()}
            pipe = self._client.pipeline()
            pipe.hmset_dict(self._key, **cache_data)
            pipe.expire(self._key, int(self._expire))
            res = await pipe.execute()
            return res[0]
        else:
            # 默认字节类型，需要编码
            return await self._client.set(self._key, json.dumps(self.data), expire=self._expire)

    async def __aenter__(self):
        self.data = await self.get()
        if self.data:
            self.update = False
        return self

    async def __aexit__(self, typ, value, traceback):
        if self.update and self.data:
            await self.set()
            return True
        if not self._except:
            return True

    def __enter__(self):
        assert ValueError("只允许异步调用！")

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert ValueError("只允许异步调用！")

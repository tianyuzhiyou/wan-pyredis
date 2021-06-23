# -*- coding: utf-8 -*-

"""
redis同步缓存代理封装
"""

__all__ = ["RedisCacheProxy"]

import json
import time
import functools
import hashlib
from contextlib import contextmanager


class RedisCacheProxy(object):

    def __init__(self, cache=None):
        """
        @desc redis代理对象，在redis包基础上更友好的封装
        :param cache:
        for example:
        pool = redis.ConnectionPool()
        cache = RedisCacheProxy(cache=pool)
        """
        self._client = cache

    @contextmanager
    def with_cache(self, key, ex=30, is_json=False, is_update=True, is_except=False):
        """
        @desc 将SimpleCache封装成上下文管理器，便于使用
        :param key: 缓存key
        :param ex: 缓存失效时间，单位s
        :param is_json: 是否将值转化成json格式进行存储， 默认否
        :param is_update: 是否在缓存穿透后自动加载缓存， 默认是
        :param is_except: 是否发生错误时抛出异常，默认否
        :return:
        for example:

        with RedisCacheProxy().with_cache("name") as cache:
            if cache.data:
                return cache.data
            data = "do something"
            cache.data = data
            return cache.data
        """
        s_cache = self.simple_cache(key, ex=ex, is_json=is_json, is_update=is_update, is_except=is_except)
        try:
            s_cache.data = s_cache.get()
            if s_cache.data:
                s_cache.update = False
            yield s_cache
        finally:
            if s_cache.update and s_cache.data:
                s_cache.set()

    def simple_cache(self,
                     key,
                     ex,
                     is_json=False,
                     is_update=True,
                     is_except=False):
        """获取一个SimpleCache对象"""
        return SimpleCache(key,
                           ex=ex,
                           cache=self._client,
                           is_json=is_json,
                           is_update=is_update,
                           is_except=is_except)

    @contextmanager
    def with_lock(self, key, expire=30, step=0.03):
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
            t = self.acquire_lock(key, expire, step)
            yield t
        finally:
            self.release_lock(key)

    def check_lock(self, key):
        """
        检查当前KEY是否有锁
        """
        key = 'lock:%s' % key
        if self._client.get(key):
            return True
        else:
            return False

    def acquire_lock(self, key, expire=30, step=0.03):
        """
        为当前KEY加锁, 默认30秒自动解锁
        """
        key = 'lock:%s' % key
        while 1:
            get_stored = self._client.get(key)
            if get_stored:
                time.sleep(step)
            else:
                if self._client.setnx(key, 1):
                    self._client.expire(key, expire)
                    return True

    def release_lock(self, key):
        """
        释放当前KEY的锁
        """
        key = 'lock:%s' % key
        self._client.delete(key)

    def get(self, key):
        """获取字符串类型"""
        return self._client.get(key)

    def set(self, key, value, ex=10):
        """保存字符串类型数据"""
        return self._client.set(key, value, ex=ex)

    def get_many(self, keys):
        """批量获取字符串数据"""
        assert isinstance(keys, list)
        return self._client.mget(*keys)

    def set_many(self, data):
        """批量设置字符串类型"""
        assert isinstance(data, dict)
        return self._client.mset(data)

    def get_data(self, key):
        """获取字符串数据并尝试转换json"""
        value = self._client.get(key)
        if value:
            try:
                value = json.loads(value)
            except:
                pass
        return value

    def set_data(self, key, value, ex=None):
        """尝试转正json字符串存储"""
        try:
            value = json.dumps(value)
        except:
            pass
        return self._client.set(key, value, ex=ex)

    def delete(self, key):
        """删除一个缓存key"""
        return self._client.delete(key)

    def delete_many(self, keys):
        """批量删除缓存key"""
        assert isinstance(keys, list)
        return self._client.delete(*keys)

    @property
    def client(self):
        return self._client

    def exists(self, key):
        """返回一个key是否存在"""
        return self._client.exists(key)

    def hget(self, key, field):
        """获取hash类型一个键值"""
        return self._client.hget(key, field)

    def hmget(self, key, fields):
        """
        批量获取hash类型键值
        :param key:
        :param fields:
        :return:
        """
        assert isinstance(fields, list)
        return self._client.hmget(key, fields)

    def hmget2dict(self, key, fields):
        """
        获取缓存hash类型键值,返回一个dict
        :param key:
        :param fields:
        """
        cache_list = self._client.hmget(key, fields)
        return dict(zip(fields, cache_list))

    def hset(self, key, field, value, ex=None):
        """设置一个hash类型key"""
        pipe = self._client.pipeline()
        pipe.hset(key, field, value)
        if ex:
            pipe.expire(key, int(ex))
        return pipe.execute()

    def hmset(self, key, values, ex=None):
        """批量设置hash类型field"""
        assert isinstance(values, dict)
        pipe = self._client.pipeline()
        pipe.hmset(key, values)
        if ex:
            pipe.expire(key, int(ex))
        return pipe.execute()

    def hexists(self, key, field):
        """判断一个hash类型field是否存在"""
        return self._client.hexists(key, field)

    def hdel(self, key, field):
        """删除一个hash类型field"""
        return self._client.hdel(key, field)

    def hmdel(self, key, fields):
        """批量删除hash类型field"""
        assert isinstance(fields, list)
        return self._client.hdel(key, *fields)

    def hkeys(self, key):
        """获取一个hash类型所有的key"""
        return self._client.hkeys(key)

    def hvals(self, key):
        """获取一个hash类型所有的values"""
        return self._client.hvals(key)

    def hgetall(self, key):
        """获取一个hash类型所有的键值对"""
        return self._client.hgetall(key)

    def get_json(self, key):
        """
        @desc 获取hash类型存储的大批量键值对
        """
        data = self._client.hgetall(key)
        if data:
            return {key: json.loads(val) for key, val in dict(data).items()}
        return {}

    def set_json(self, key, value, ex=None):
        """
        @desc 设置hash类型存储的大批量键值对
        :param key:
        :param value:
        :param ex:
        :return:
        """
        assert isinstance(value, dict)
        cache_data = {k: json.dumps(v) for k, v in value.items()}
        pipe = self._client.pipeline()
        pipe.hmset(key, cache_data)
        if ex:
            pipe.expire(key, int(ex))
        return pipe.execute()

    def delete_memoize(self, key, cache_per="", is_safe=True, *args, **kwargs):
        """删除缓存的结果值"""
        cache_key = cache_per + key + ":" + self._make_cache_key(*args, **kwargs)
        if is_safe:
            self._client.expire(cache_key, -1)
        else:
            self._client.delete(cache_key)

    def memoize(self,
                key,
                ex=30,
                cache_per="",
                is_json=False,
                is_update=False,
                exception=False):
        """
        @desc 对有参函数结果进行缓存的装饰器,
        如果缓存获取异常，直接缓存穿透
        for example:
        @RedisCacheProxy().memoize("key")
        def test(a, b):
            "do something"

        :param key: 缓存键
        :param ex: 失效时间
        :param is_json: 是否需要json优化
        :param is_update: 是否获取的同时更新缓存
        :param cache_per: 缓存前缀
        :param exception: 是否抛出异常
        :return:
        """

        def memoize(f):
            @functools.wraps(f)
            def decorated_function(*args, **kwargs):
                cache_key = cache_per + key + ":" + self._make_cache_key(*args, **kwargs)
                get_func = self.get_json if is_json else self.get_data
                set_func = self.set_json if is_json else self.set_data
                try:
                    cache_data = get_func(cache_key)
                    if is_update:
                        set_func(cache_key, cache_data, ex=ex)
                    if cache_data:
                        return cache_data
                except Exception as e:
                    if exception:
                        raise ValueError(e)
                    return f(*args, **kwargs)
                cache_data = f(*args, **kwargs)
                set_func(cache_key, cache_data, ex=ex)
                return cache_data

            return decorated_function

        return memoize

    def _make_cache_key(self, *args, **kwargs):
        """
        @desc 生成缓存key
        :return: str
        """
        l_data = filter(lambda x: x.__class__ in {str, int} and x, list(args) + list(kwargs.values()))
        value = ":" + ",".join(sorted([str(i).replace("-", "") for i in l_data]))
        m = hashlib.md5()
        m.update(value.encode("utf-8"))
        return m.hexdigest()


class SimpleCache(object):
    """只针对字符串和json的缓存上下文管理器"""

    def __init__(self,
                 key,
                 ex=30,
                 cache=None,
                 is_json=False,
                 is_update=True,
                 is_except=True):
        self._key = key
        self._expire = ex
        self._client = cache
        self.data = None
        self.json = is_json
        self.update = is_update
        self._except = is_except

    def __enter__(self):
        self.data = self.get()
        if self.data:
            self.update = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.update and self.data:
            self.set()
        if not self._except:
            return True

    def get(self):
        if self.json:
            data = self._client.hgetall(self._key)
            if data:
                return {key: json.loads(val) for key, val in dict(data).items()}
            return {}
        else:
            value = self._client.get(self._key)
            if not value:
                return {}
            return json.loads(value)

    def set(self):
        if self.json:
            assert isinstance(self.data, dict)
            cache_data = {k: json.dumps(v) for k, v in self.data.items()}
            pipe = self._client.pipeline()
            pipe.hmset(self._key, cache_data)
            pipe.expire(self._key, int(self._expire))
            return pipe.execute()[0]
        else:
            return self._client.set(self._key, json.dumps(self.data), ex=self._expire)

    def exists(self):
        return self._client.exists(self._key)

# -*- coding: utf-8 -*-
import hashlib

class PfCache(object):
    def __init__(self, cache=None):
        """
        @desc 封装redis的HyperLoglog结构，
        这是是一种概率数据结构，
        只能用在需要统计不那么精确的结果,
        比如：一篇文章的查看不重复人数
        :param cache:
        """
        self._client = cache

    def _get_signature(self, *args):
        """
        @desc 获取签名
        :param key:
        :return:
        """
        if not args:
            return ""
        value = "".join(args)
        m = hashlib.md5()
        m.update(value.encode("utf-8"))
        return m.hexdigest()

    def add(self, key, *args, **kwargs):
        """
        @desc 向结构中添加数据
        :param key:
        :param args:[[],[]]
        :param kwargs: ex=失效时间/s
        :return: bool, 结构是否发生改变，即是否已添加
        """
        if not args:
            return False
        ex = kwargs.pop("ex", None)
        values = [self._get_signature(*data) for data in args]
        b_res = self._client.pfadd(key, *values)
        if ex:
            self._client.expire(key, int(ex))
        return bool(b_res)

    def count(self, key, *args):
        """
        @desc 获取历史所有的添加数据的个数
        :param key:
        :param args:key1, key2
        :return:
        """
        keys = [key]
        if args:
            keys += args
        return self._client.execute_command('PFCOUNT', *keys)

    def merge(self, to, *sources):
        """
        @desc 合并形成一个新的并集
        :param to: 新的结构体
        :param sources: 源结构体列表
        :return:
        """
        return self._client.pfmerge(to, *sources)
# -*- coding: utf-8 -*-
from functools import lru_cache


@lru_cache(maxsize=2000)
def redis_get_conf(conf: dict) -> dict:
    """
    @desc 构建全局配置
    """
    return dict(
        address=conf["address"],
        maxsize=conf["maxsize"],
        minsize=conf["minsize"],
        timeout=conf["timeout"],
        password=conf["password"],
        db=conf["db_index"],
        encoding=conf["encoding"]
    )

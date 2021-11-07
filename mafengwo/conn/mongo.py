#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/11/3 2:55 下午
# @File: mongo.py


# 第三方库
import pymongo.results
from pymongo import MongoClient


class MongoBase(object):
    """Base class that mongodb database operation implementations derive from"""

    def __init__(self, db_name: str, collection_name: str):
        """Declare variable"""
        # 声明 self.client
        self.client = MongoClient()
        # 声明 self.db
        self.db = self.client[db_name]
        # 声明 collection_name
        self.collection_name = self.db[collection_name]

    def add_many(self, infos: list) -> pymongo.results.InsertManyResult:
        """
        Insert data in mongodb database.

        :param infos: 包含最终数据的列表
        """
        # 输出 log 信息
        print("Method add_many: loading the module of add_many ...")
        # 捕获异常
        try:
            return self.collection_name.insert_many(infos)
        except Exception as e:
            # 输出 log 信息
            print(f"Method add_many: the error of inserting data: {e}")
        finally:
            # 关闭连接
            self.client.close()
            # 输出 log 信息
            print("Method add_many: exiting the module of add_many ...")

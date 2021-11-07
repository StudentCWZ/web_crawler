#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:39 下午
# @File: read_conf.py


# 基本库
import os
import sys

# 第三方库
import yaml


class ConfBase(object):
    """Base class that all reading configure implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # 获取 self.dir_path
        self.dir_path = os.path.split(os.path.realpath(__file__))[0]
        # 获取 self.file_path
        self.file_path = os.path.join(self.dir_path, "conf.yaml")
        # 声明 cf 对象
        self.cf = yaml.load(open(self.file_path, encoding="utf-8"), Loader=yaml.FullLoader)

    def init_mysql_conf(self) -> tuple:
        """
        Takes the configure of data about connecting mysql database.

        :return sql_cnf, table_name, create_table_sql, insert_table_sql
        """
        # 获取 sql_cnf
        sql_cnf = self.cf.get("mysql", {})
        # 条件判断
        if not sql_cnf:
            # 输出 debug 信息
            print("Method init_mysql_conf: the error of getting the configure of mysql ...")
            # 输出 debug 信息
            print("Method init_mysql_conf: exiting the program ...")
            # 退出程序
            sys.exit(1)
        # 获取 sql_dict
        sql_dict = {
            "host": sql_cnf.get("host"),
            "port": sql_cnf.get("port"),
            "user": sql_cnf.get("user"),
            "passwd": sql_cnf.get("passwd"),
            "db": sql_cnf.get("db")
        }
        # 获取 table_name
        table_name = sql_cnf.get("table_name")
        # 获取 create_table_sql
        create_table_sql = sql_cnf.get("create_table_sql").format(table_name)
        # 获取 insert_table_sql
        insert_table_sql = sql_cnf.get("insert_table_sql").format(table_name)
        # 返回 sql_dict, table_name, create_table_sql, insert_table_sql
        return sql_dict, table_name, create_table_sql, insert_table_sql

    def init_url(self) -> str:
        """
        Takes the configure of url.

        :return url: 网址
        """
        # 获取 url
        url = self.cf.get("url", {}).get("city_url", "")
        # 条件判断
        if not url:
            # 输出 debug 信息
            print("Method init_url: the error of getting the configure of url ...")
            # 输出 debug 信息
            print("Method init_url: exiting the program ...")
            # 退出程序
            sys.exit(1)
        # 返回 url
        return url

    def init_cities(self) -> list:
        """
        Takes the configure of cities.

        :return cities_list: 城市信息列表
        """
        # 获取 cities
        cities = self.cf.get("city", {}).get("cities", "")
        # 获取 cities_list
        cities_list = cities.split(",")
        # 条件判断
        if not cities_list:
            # 输出 debug 信息
            print("Method init_cities: the error of getting the configure of cities ...")
            # 输出 debug 信息
            print("Method init_cities: exiting the program ...")
            # 退出程序
            sys.exit(1)
        # 返回 cities_list
        return cities_list

    def init_key(self) -> list:
        """
        Takes the configure of key words.

        :return key_list: 关键字信息列表
        """
        # 获取 key
        key = self.cf.get("key", {}).get("key_list", "")
        # 获取 key_list
        key_list = key.split(",")
        # 条件判断
        if not key_list:
            # 输出 debug 信息
            print("Method init_key: the error of getting the configure of key ...")
            # 输出 debug 信息
            print("Method init_key: exiting the program ...")
            # 退出程序
            sys.exit(1)
        # 返回 key_list
        return key_list

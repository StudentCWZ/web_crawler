#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:36 下午
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
        # 声明 dir_path
        self.dir_path = os.path.split(os.path.realpath(__file__))[0]
        # 声明 file_path
        self.file_path = os.path.join(self.dir_path, "conf.yaml")
        # 获取 cf 对象
        self.cf = yaml.load(open(self.file_path, encoding="utf-8"), Loader=yaml.FullLoader)

    def mysql_conn_conf(self) -> dict:
        """
        Takes the configure of data by connecting mysql database.

        :return: mysql_conf
        """
        # 获取 mysql_conf
        mysql_conf = self.cf.get("mysql", {})
        # 条件判断
        if mysql_conf:
            # 条件判断
            if "table_name" in mysql_conf:
                # 删除键值对
                mysql_conf.pop("table_name")
            # 条件判断
            if "create_table_sql" in mysql_conf:
                # 删除键值对
                mysql_conf.pop("create_table_sql")
            # 条件判断
            if "insert_table_sql" in mysql_conf:
                # 删除键值对
                mysql_conf.pop("insert_table_sql")
            # 返回 mysql_conf
            return mysql_conf
        else:
            # 输出 log 信息
            print("Method mysql_conn_conf: the error of getting configure file!")
            # 退出程序
            sys.exit(1)

    def mysql_use_conf(self) -> tuple:
        """
        Takes the configure of data by using mysql database.

        :return: table_name, create_table_sql, insert_table_sql
        """
        # 获取 mysql_conf
        mysql_conf = self.cf.get("mysql", {})
        # 条件判断
        if mysql_conf:
            # 获取 table_name
            table_name = mysql_conf.get("table_name", "")
            # 获取 create_table_sql
            create_table_sql = mysql_conf.get("create_table_sql", "").format(table_name)
            # 获取 insert_table_sql
            insert_table_sql = mysql_conf.get("insert_table_sql", "").format(table_name)
            # 返回 table_name, create_table_sql, insert_table_sql
            return table_name, create_table_sql, insert_table_sql
        else:
            # 输出 log 信息
            print("Method mysql_use_conf: the error of getting configure file!")
            # 退出程序
            sys.exit(1)

    def url_conf(self) -> str:
        """
        Takes the configure of url.

        :return: url: 网址
        """
        # 获取 mysql_conf
        url = self.cf.get("url", {}).get("initial_url", "")
        # 条件判断
        if not url:
            # 输出 debug 信息
            print("Method url_conf: the error of getting url ...")
            # 程序异常：退出程序
            sys.exit(1)
        # 返回 url
        return url


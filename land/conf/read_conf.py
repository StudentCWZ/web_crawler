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
        Takes the configure of data by connecting mysql database.

        :return: sql_cnf, table_name, create_table_sql, insert_table_sql
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


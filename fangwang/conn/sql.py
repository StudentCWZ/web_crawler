#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:39 下午
# @File: sql.py


# 基本库
import logging
import re

# 第三方库
import pymysql
import dbutils
from dbutils.persistent_db import PersistentDB


class MysqlBase(object):
    """Base class that all database operation implementations derive from"""

    def __init__(self, conf: dict):
        """Declare variable"""
        # logging 模块初始化
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        # 声明 conf
        self.conf = conf

    def _conn_sql(self) -> dbutils.steady_db.SteadyDBConnection:
        """
        Connect mysql database.

        :return: conn: 一个 conn 对象
        """
        # 捕获异常
        try:
            # 获取 pool
            pool = PersistentDB(pymysql, **self.conf)
            # 从数据库连接池是取出一个数据库连接
            conn = pool.connection()
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method _conn_sql: the error of connecting database: {e}")
        else:
            # 输出 log 信息
            logging.info("Method _conn_sql: the process of connecting database is successful!")
            # 返回 conn
            return conn

    def table_exists(self, table_name: str) -> bool:
        """
        Judge the existence of data sheet.

        :param table_name
        :return: True/False
        """
        # 获取 conn 对象
        conn = self._conn_sql()
        # 获取 cursor 对象
        with conn.cursor() as cursor:
            # sql 语句
            sql = "show tables;"
            # 捕获异常
            try:
                # 执行 sql 语句
                cursor.execute(sql)
            except Exception as e:
                # 输出 log 信息
                logging.error(f"Method table_exists: the error of executing sql: {e}")
            # 获取 tables
            tables = [cursor.fetchall()]
            # 捕获异常
            try:
                # 获取 table_list
                table_list = re.findall('(\'.*?\')', str(tables))
                # 修改 table_list
                table_list = [re.sub("'", '', each) for each in table_list]
            except Exception as e:
                # 输出 log 信息
                logging.error(f"Method table_exists: the error of getting table_list and revising table_list: {e}")
            # 捕获异常
            try:
                # 条件判断
                if table_name in table_list:
                    # 输出 log 信息
                    logging.info("Method table_exists: the table of %s existed!" % table_name)
                    # 返回 True
                    return True
                else:
                    # 输出 log 信息
                    logging.info("Method table_exists: the table of %s does not existed!" % table_name)
                    # 返回 False
                    return False
            except Exception as e:
                # 输出 log 信息
                logging.error(f"Method table_exists: the error of judging table_list: {e}")
            finally:
                # 关闭连接
                conn.close()

    def create_table(self, table_name: str, sql: str):
        """
        Create a new data sheet in database.

        :param table_name: 数据库表
        :param sql: sql 语句
        """
        # 获取 conn 对象
        conn = self._conn_sql()
        # 获取 cursor 对象
        cursor = conn.cursor()
        # 捕获异常
        try:
            # 执行 sql 语句
            cursor.execute(sql)
            # 事务的手动提交
            conn.commit()
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method create_table: the error of executing sql: {e}")
            # 事务回滚
            conn.rollback()
        else:
            # 输出 log 日志
            logging.info("Method create_table: data sheet of %s is established!" % table_name)
        finally:
            # 关闭连接
            conn.close()

    def insert_data(self, sql: str, result_list: list):
        """
        Insert data in data sheet.

        :param sql: 插入数据 sql 语句
        :param result_list: 数据结果
        """
        # 获取 conn 对象
        conn = self._conn_sql()
        # 获取 cursor 对象
        cursor = conn.cursor()
        # 输出 log 信息
        logging.info("Method insert_data: data is inserting ...")
        # 捕获异常
        try:
            # 数据批量插入
            cursor.executemany(sql, result_list)
            # 事务的手动提交
            conn.commit()
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method insert_data: the error of insert_data: {e}")
            # 事务回滚
            conn.rollback()
        else:
            # 输出 log 信息
            logging.info("Method insert_data: the inserting of data is finished!")
        finally:
            # 关闭连接
            conn.close()


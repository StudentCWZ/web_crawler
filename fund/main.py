#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:37 下午
# @File: main.py


# 基本库
import logging
import random
import re
import sys
import time

# 第三方库
import requests
from typing import Generator

# 自定义库
from conf import read_conf as rc
from conn import sql as cs
from header import use_headers as uh


class CrawlerBase(object):
    """Base class that all dedicating data implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # logging 模块初始化
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        # 声明 self.initial_url
        self.initial_url = rc.ConfBase().init_url()
        # 声明 self.result_list
        self.result_list = list()
        # 声明 self.mysql_dict
        self.mysql_dict = rc.ConfBase().init_mysql_conf()[0]
        # data sheet
        self.table_name = rc.ConfBase().init_mysql_conf()[1]
        # sql 语句
        self.create_table_sql = rc.ConfBase().init_mysql_conf()[2]
        # sql 语句
        self.insert_table_sql = rc.ConfBase().init_mysql_conf()[3]

    @staticmethod
    def get_page(url: str) -> str:
        """
        Takes the html by requesting the web page.

        :param url: 网址
        :return: html: 网页源码
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # 增加重连次数
        requests.adapters.DEFAULT_RETRIES = 8
        # 开启会话
        s = requests.session()
        # 关闭多余连接
        s.keep_alive = False
        # 设置请求头
        s.headers = headers
        # 捕获异常
        try:
            # 请求相应的 url，获取网页信息
            response = s.get(url, timeout=200)
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_page: the error of getting response: {e}")
            # 返回 html
            return ""
        # 判断状态码
        if response.status_code != 200:
            # 输出 debug 信息
            print("Method get_page: the error of getting efficient html ...")
            # 返回 html
            return ""
        else:
            # 获取网页源码
            html = response.text
        # 随机休眠 1~5s
        time.sleep(random.randint(1, 5))
        # 返回 html
        return html

    @staticmethod
    def parse_page(html: str) -> Generator:
        """
        Parses the html from web pages.

        :param html: 网页源码
        :return: Generator: 生成器
        """
        # 获取 table 标签内容
        table_list = re.findall(r'<table.*?id="tb">.*?<tr align="center".*?>.*?</tr>(.*?)</table>', html, re.S)
        # 条件判断
        if not table_list:
            # 输出 debug 信息
            print("Method parse_page: the error of getting table_list ...")
        # 遍历
        for table in table_list:
            # 获取 tr 标签
            fund_tr = re.findall(r'<tr align="center".*?>(.*?class="fundId".*?)</tr>', table, re.S)
            # 条件判断
            if not fund_tr:
                # 跳出循环
                continue
            # 遍历
            for tr in fund_tr:
                # 获取 tr 标签内容
                fund_abbreviation_group = re.search(r'onclick="cookieset.*?>(.*?)</a>', tr, re.S)
                # 条件判断
                if not fund_abbreviation_group:
                    # 数据替换
                    fund_abbreviation = ""
                else:
                    # 获取 fund_abbreviation
                    fund_abbreviation = fund_abbreviation_group.group(1)
                # 获取 td 标签内容
                fund_td = re.findall(r'<td.*?>(.*?)</td>', tr, re.S)
                # 条件判断
                if not fund_td:
                    # 跳入下一个循环
                    continue
                # 切片操作
                fund_td = fund_td[2:]
                # 生成器
                yield {
                    "fund_abbreviation": fund_abbreviation,
                    "fund_id": fund_td[0].strip(),
                    "fund_net_worth_date": fund_td[1],
                    "fund_net_worth": fund_td[2],
                    "fund_cumulative_net_worth": fund_td[3],
                    "fund_quote_change": fund_td[4].strip(),
                    "fund_establishment_date": fund_td[5],
                    "fund_subscription_status": fund_td[6],
                    "fund_redemption_status": fund_td[7],
                    "fund_fixed_investment_status": fund_td[8]
                }

    def main(self):
        """The method is entrance of program."""
        # 输出 log 信息
        print("################################# Running fund.py ##################################")
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # 获取网页信息
        html = self.get_page(self.initial_url)
        # 条件判断
        if not html:
            # 输出 debug 信息
            print("Method main: the html is an emtpy string ...")
            # 程序异常：退出程序
            sys.exit(1)
        # 遍历
        for item in self.parse_page(html):
            # 条件判断
            if not item:
                # 跳入下个循环
                continue
            # 输出 item_tuple
            item_tuple = (item["fund_abbreviation"], item["fund_id"], item["fund_net_worth_date"],
                          item["fund_net_worth"], item["fund_cumulative_net_worth"], item["fund_quote_change"],
                          item["fund_establishment_date"], item["fund_subscription_status"],
                          item["fund_redemption_status"], item["fund_fixed_investment_status"])
            # 列表添加元素
            self.result_list.append(item_tuple)
        # 条件判断
        if not self.result_list:
            # 输出 debug 信息
            print("Method main: the error of getting result_list ...")
            # 程序异常：退出程序
            sys.exit(1)
        # 输出 log 信息
        logging.info("The process of Requesting and Parsing is successful!")
        # 输出 log 信息
        print("------------------------------------------------------------------------------------")
        # 输出 log 信息
        logging.info("Operating database ...")
        # 实例化 MysqlBase 对象，用于后续的数据插入 mysql
        mb = cs.MysqlBase(conf=self.mysql_dict)
        # 条件判断
        if mb.table_exists(self.table_name):
            # 数据插入
            mb.insert_data(self.insert_table_sql, self.result_list)
        else:
            # 新建 data sheet
            mb.create_table(self.table_name, self.create_table_sql)
            # 数据插入
            mb.insert_data(self.insert_table_sql, self.result_list)
        # 输出 log 信息
        logging.info("The process of operating database is successful!")
        # 输出 log 信息
        print("################################# Exiting fund.py ##################################")


if __name__ == '__main__':
    # 实例化 CrawlerBase 对象
    cb = CrawlerBase()
    # 调用 main 函数
    cb.main()


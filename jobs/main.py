#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 4:27 下午
# @File: main.py


# 基本库
import json
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
        # 声明 self.city_url
        self.city_url = rc.ConfBase().init_url()
        # 声明 self.cities_list
        self.cities_list = rc.ConfBase().init_cities()
        # 声明 self.keyword_list
        self.keyword_list = rc.ConfBase().init_key()
        # 声明 self.result_list
        self.result_list = list()
        # 声明 self.mysql_dict
        self.mysql_dict = rc.ConfBase().init_mysql_conf()[0]
        # 声明 table_name
        self.table_name = rc.ConfBase().init_mysql_conf()[1]
        # sql 语句
        self.create_table_sql = rc.ConfBase().init_mysql_conf()[2]
        # sql 语句
        self.insert_table_sql = rc.ConfBase().init_mysql_conf()[3]
        # 声明 file_name
        self.file_name = os.path.split(__file__)[-1]

    def __enter__(self):
        # 输出 log 信息
        print("############################### Running {} ################################".format(self.file_name))
        # 返回 self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 输出 log 信息
        print("############################### Exiting {} ################################".format(self.file_name))
        # 返回 False
        return False

    def get_cities(self) -> dict:
        """
        Takes the list of cities.

        :return area_dict: 城市数据
        """
        # 随机获取请求头
        city_headers = {"User-Agent": random.choice(uh.HeadersBase().get_headers())}
        # 捕获异常
        try:
            # 发起 get 请求
            response = requests.get(url=self.city_url, headers=city_headers)
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_cities: the error of getting response: {e}")
            # 程序异常：退出程序
            sys.exit(1)
        # 条件判断
        if response.status_code != 200:
            # 输出 debug 信息
            print("Method get_cities: the error of getting efficient html ...")
            # 程序异常：退出程序
            sys.exit(1)
        else:
            # 获取网页源码
            html = response.text
        # 捕获异常
        try:
            # 获取 area
            area = re.findall(r"area=(.*?);", html, re.S)[0]
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_cities: the error of getting area: {e}")
        else:
            # 字符串转为字典
            area_dict = json.loads(area)
            # 键值互换
            area_dict = {value: key for key, value in area_dict.items()}
            # 返回 area_dict
            return area_dict

    @staticmethod
    def max_page(city_code: str, keyword: str) -> int:
        """
        Takes the numbers of web pages.

        :param city_code: 城市对应的数值
        :param keyword: 关键词
        :return int(max_page): 最大翻页
        """
        # url 拼接
        url = "https://search.51job.com/list/" + str(city_code) + ",000000,0000,00,9,99," + str(keyword) + ",2,1.html"
        # 声明 main_headers
        main_headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers()),
            "Host": "search.51job.com",
            "Upgrade-Insecure-Requests": "1"
        }
        # 捕获异常
        try:
            # 发起 get 请求
            response = requests.get(url=url, headers=main_headers)
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method max_page: the error of getting response: {e}")
            # 程序异常：退出程序
            sys.exit(1)
        # 条件判断
        if response.status_code != 200:
            # 输出 debug 信息
            print("Method max_page: the error of getting efficient html ...")
            # 程序异常：退出程序
            sys.exit(1)
        else:
            # 获取网页源码
            html = response.text
        # 休眠
        time.sleep(random.randint(5, 10))
        # 捕获异常
        try:
            # 正则表达式获取信息
            max_page = re.findall(r'"total_page":"(.*?)",', html, re.S)[0]
        except Exception as e:
            # 输出 log 日志
            logging.error(f"Method max_page: the error of getting max_page: {e}")
        else:
            # 返回 int(max_page)
            return int(max_page)

    @staticmethod
    def get_page(city_code: str, keyword: str, page: int) -> str:
        """
        Takes the html by requesting the web page.

        :param city_code: 城市对应的数值
        :param keyword: 关键词
        :param page: 页数
        :return html: 网页源码
        """
        # 声明 main_headers
        main_headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers()),
            "Host": "search.51job.com",
            "Upgrade-Insecure-Requests": "1"
        }
        # url 拼接
        url = "https://search.51job.com/list/" + str(city_code) + ",000000,0000,00,9,99," + str(keyword) + ",2," + \
              str(page) + ".html"
        # 捕获异常
        try:
            # 发起 get 请求
            response = requests.get(url=url, headers=main_headers)
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_page: the error of getting response: {e}")
            # 程序异常：退出程序
            sys.exit(1)
        # 条件判断
        if response.status_code != 200:
            # 输出 debug 信息
            print("Method get_page: the error of getting efficient html ...")
            # 程序异常：退出程序
            sys.exit(1)
        else:
            # 获取网页源码
            html = response.text
        # 休眠
        time.sleep(random.randint(5, 10))
        # 返回 html
        return html

    @staticmethod
    def details_page(html: str) -> Generator:
        """
        Parses the html from web pages.

        :param html: 网页源码
        :return Generator: 生成器
        """
        # 捕获异常
        try:
            # 正则表达式获取信息
            data_str = re.findall(r'window.__SEARCH_RESULT__ = ({.*?})</script>', html, re.S)[0]
        except Exception as e:
            # 输出 log 日志
            logging.error(f"Method details_page: the error of getting data_str: {e}")
        else:
            # 字符串转为字典
            data_dict = json.loads(data_str)
            # 遍历
            for item in data_dict["engine_jds"]:
                # generator
                yield item

    def main(self):
        """The method is entrance of program"""
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # 遍历
        for city_name in self.cities_list:
            # 获取 city_code
            city_code = self.get_cities().get(city_name)
            # 遍历
            for keyword in self.keyword_list:
                # 获取 max_page
                max_page = self.max_page(city_code, keyword)
                # 条件判断
                if max_page is None:
                    # 跳出循环
                    continue
                # for 循环
                for page in range(1, max_page):
                    # 输出 log 信息
                    print("城市：", city_name)
                    # 输出 log 信息
                    print("职位：", keyword)
                    # 输出 log 信息
                    print("请求页码：", str(page))
                    # 获取网页源码
                    html = self.get_page(city_code, keyword, page)
                    # 条件判断
                    if html is None:
                        # 跳出循环
                        continue
                    # 遍历
                    for item in self.details_page(html):
                        # 捕获异常
                        try:
                            # 获取 item_tuple
                            item_tuple = (item["type"], item["jobid"], item["coid"], item["job_href"], item["job_name"],
                                          item["company_href"], item["company_name"], item["providesalary_text"],
                                          item["workarea_text"], item["companytype_text"],
                                          item["issuedate"], item["jobwelf"], "/".join(item["attribute_text"]),
                                          item["companysize_text"], item["companyind_text"])
                        except Exception as e:
                            # 输出 log 日志
                            logging.error(f"Method main: the error of getting item_tuple: {e}")
                        else:
                            print(item_tuple)
                            # 列表添加元素
                            self.result_list.append(item_tuple)
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
            mb.new_table(self.table_name, self.create_table_sql)
            # 数据插入
            mb.insert_data(self.insert_table_sql, self.result_list)
        # 输出 log 信息
        logging.info("The process of operating database is successful!")


if __name__ == '__main__':
    # 上下文管理器
    with CrawlerBase() as cb:
        # 调用 main 方法
        cb.main()


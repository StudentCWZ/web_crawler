#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:36 下午
# @File: main.py


# 基本库
import json
import logging
import sys
import random
import time

# 第三方库
import requests
from typing import Generator

# 自定义
from conf import read_conf as rc
from conn import sql as cs
from header import use_headers as uh
from proxies import proxies_pool as pp


class CrawlerBase(object):
    """Base class that all dedicating data implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # logging 模块初始化
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        # 定义 self.initial_url
        self.initial_url = rc.ConfBase().url_conf()
        # 声明 self.city_data_list
        self.city_data_list = list()
        # 声明 self.result_list
        self.result_list = list()
        # 声明 self.mysql_dict
        self.mysql_dict = rc.ConfBase().mysql_conn_conf()
        # 声明 table_name, create_table_sql, insert_table_sql
        self.table_name, self.create_table_sql, self.insert_table_sql = rc.ConfBase().mysql_use_conf()

    @staticmethod
    def get_page(url: str, proxies_list: list) -> dict:
        """
         Takes the html by requesting the web page.

        :param url: 网页源码
        :param proxies_list: 代理 ip 列表
        :return: html_dict: 字典
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # 随机获取代理IP
        proxies = random.choice(proxies_list)
        # 增加重连次数
        requests.adapters.DEFAULT_RETRIES = 8
        # 创建会话
        s = requests.session()
        # 关闭多余连接
        s.keep_alive = False
        # 添加代理
        s.proxies = proxies
        # 添加请求头
        s.headers = headers
        # 捕获异常
        try:
            # 请求网页
            response = s.get(url, timeout=100)
            # 判断状态码
            if response.status_code == 200:
                # 获取网页源码
                html = response.text
                # 将 str 转换为 dict
                html_dict = json.loads(html)
                # 随机休眠 1~5s
                time.sleep(random.randint(1, 5))
                # 返回 html_dict
                return html_dict
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_page: The error of getting response: {e}")

    @staticmethod
    def parse_page(city_name: str, html_dict: dict) -> Generator:
        """
        Parses the html from web pages.

        :param city_name: 城市名
        :param html_dict: 网页源码
        :return: Generator
        """
        # 捕获异常
        try:
            # 遍历
            for data in html_dict["data"]["data"]:
                # 获取 date
                date = data["forecast_date"]
                # 遍历
                for weather in data["forecast_data"]:
                    # 条件判断
                    if weather["daynight"] == 0:
                        # 变量赋值
                        day_night = "day"
                    else:
                        # 变量赋值
                        day_night = "night"
                    # 生成器
                    yield {
                        "city": city_name,
                        "date": date,
                        "weather": weather["weather_name"],
                        "day_night": day_night,
                        "temp_min": weather["min_temp"],
                        "temp_max": weather["max_temp"]
                    }
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method parse_page: The error of parsing page: {e}")

    def main(self):
        """The method is entrance of program."""
        # 输出 log 信息
        logging.info("Getting proxies ...")
        # 所有代理 ip 列表
        all_proxies_list = pp.ProxiesBase().all_proxies(uh.HeadersBase().get_headers())
        # 有效代理 ip 列表
        proxies_list = pp.ProxiesBase().verify_proxies(all_proxies_list)
        # 输出 log 信息
        logging.info("Getting proxies is successful!")
        # 输出 log 信息
        print("------------------------------------------------------------------------------------")
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # 获取网页信息
        html_dict = self.get_page(url=self.initial_url, proxies_list=proxies_list)
        # 条件判断
        if not html_dict.get("data", {}).get("cityByLetter", {}):
            # 退出函数
            sys.exit()
        # 获取城市信息的字典
        city_data_dict = html_dict["data"]["cityByLetter"]
        # 遍历列表
        for lst in city_data_dict.values():
            # 获取城市信息
            self.city_data_list += lst
        # 遍历列表
        for city_data in self.city_data_list:
            # 拼接 url
            url = "https://www.amap.com/service/weather?adcode=" + city_data["adcode"]
            # 获取城市名称
            city_name = city_data["name"]
            # 随机休眠 1~3s
            time.sleep(random.randint(1, 3))
            # 获取网页信息
            html_dict = self.get_page(url=url, proxies_list=proxies_list)
            # 条件判断
            if not html_dict:
                # 跳过本次循环
                continue
            # 解析网页信息
            for item in self.parse_page(city_name, html_dict):
                # 输出最终信息
                print(item)
                # 获取 item_tuple
                item_tuple = (item["city"], item["date"], item["weather"], item["day_night"], item["temp_min"],
                              item["temp_max"])
                # 列表添加元素
                self.result_list.append(item_tuple)
        # 输出 log 信息
        logging.info("The process of Requesting and Parsing is successful!")
        # 输出 log 信息
        print("------------------------------------------------------------------------------------")
        # 输出 log 信息
        logging.info("Operating database ...")
        # 实例化 MysqlBase 对象
        mb = cs.MysqlBase(self.mysql_dict)
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


if __name__ == '__main__':
    # 实例化 CrawlerBase 对象
    cb = CrawlerBase()
    # 调用 main 函数
    cb.main()


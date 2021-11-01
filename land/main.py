#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 4:27 下午
# @File: main.py


# 基本库
import sys
import logging
import random
import time

# 第三方库
from lxml import etree
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
        # 定义 self.result_list
        self.result_list = list()
        # 声明 self.mysql_dict
        self.mysql_dict = rc.ConfBase().init_mysql_conf()[0]
        # 声明 table_name
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
            response = s.get(url, timeout=100)
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
        # etree 一个对象
        html = etree.HTML(html)
        # 获取 li_list
        li_list = html.xpath('//div[@class="land-l-cont"]/dl')
        # 条件判断
        if not li_list:
            # 输出 debug 信息
            print("Method parse_page: the error of getting li_list ...")
        # 遍历
        for li in li_list:
            # 捕获异常
            try:
                # 土地位置
                location = li.xpath('.//dd/p[7]/text()')[0]
                # 出让形式
                transfer_form = li.xpath('.//dt/i/text()')[0]
                # 推出时间
                launch_time = li.xpath('.//dd/p[1]/text()')[0]
                # 土地面积
                land_area = li.xpath('.//dd/p[3]/text()')[0]
                # 规划建筑面积
                planning_area = li.xpath('.//dd/p[5]/text()')[0]
                # 土地地址
                address = li.xpath('.//dd/p[4]/text()')[0]
                # 成交状态
                state = li.xpath('.//dd/p[2]/text()')[0]
                # 土地代号
                area_code = li.xpath('.//dt/span/text()')[0]
                # 规划用途
                planned_use = li.xpath('.//dd/p[6]/text()')[0]
            except Exception as e:
                # 输出 log 信息
                logging.error(f"Method parse_page: the error of getting field: {e}")
            else:
                # 生成器
                yield {
                    "location": location,
                    "transfer_form": transfer_form,
                    "launch_time": launch_time,
                    "land_area": land_area,
                    "planning_area": planning_area,
                    "address": address,
                    "state": state,
                    "area_code": area_code,
                    "planned_use": planned_use
                }

    def main(self):
        """The method is entrance of program."""
        # 输出 log 信息
        print("################################# Running main.py ##################################")
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # for 循环
        for i in range(1, 101):
            # 输出 log 信息
            logging.info(f"正在抓取第 {i} 页")
            # 构造 url
            url = f"https://www.tudinet.com/market-212-0-0-0/list-pg{i}.html"
            # 获取网页源码
            html = self.get_page(url)
            # 条件判读
            if not html:
                # 跳入下个循环
                continue
            # 随机休眠(防止爬得过快 给服务器减少压力)
            time.sleep(random.uniform(3, 5))
            # 遍历
            for item in self.parse_page(html):
                # 条件判断
                if not item:
                    # 跳入下次循环
                    continue
                # 输出 log 信息
                # print(item)
                # 获取 item_tuple
                item_tuple = (item["location"], item["transfer_form"], item["launch_time"], item["land_area"],
                              item["planning_area"], item["address"], item["state"],
                              item["area_code"], item["planned_use"])
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
        # 输出 log 信息
        print("################################# Exiting main.py ##################################")


if __name__ == '__main__':
    # 实例化 CrawlerBase 对象
    cb = CrawlerBase()
    # 调用 main 函数
    cb.main()


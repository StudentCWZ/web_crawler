#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/11/8 10:37 上午
# @File: main.py


# 基本库
import logging
import os
import random
import re
import sys
import time

# 第三方库
import requests
from multiprocessing import Pool
from typing import Generator

# 自定义库
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
        # 声明 self.initial_url
        self.initial_url = rc.ConfBase().url_conf()
        # 所有代理 ip 列表
        self.all_proxies_list = pp.ProxiesBase().all_proxies(uh.HeadersBase().get_headers())
        # 有效代理 ip 列表
        self.proxies_list = pp.ProxiesBase().verify_proxies(self.all_proxies_list)
        # 声明 self.result_list
        self.result_list = list()
        # 声明 self.mysql_dict
        self.mysql_dict = rc.ConfBase().mysql_conn_conf()
        # data sheet
        self.table_name = rc.ConfBase().mysql_use_conf()[0]
        # sql 语句
        self.create_table_sql = rc.ConfBase().mysql_use_conf()[1]
        # sql 语句
        self.insert_table_sql = rc.ConfBase().mysql_use_conf()[2]
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

    def get_page(self, url: str) -> str:
        """
        Takes html from web page.

        :param url: 网页链接
        :return html: 网页源码
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # 随机选取代理 IP
        proxies = random.choice(self.proxies_list)
        # 增加重连次数
        requests.adapters.DEFAULT_RETRIES = 8
        # 开启会话
        s = requests.session()
        # 关闭多余连接
        s.keep_alive = False
        # 设置代理 IP
        s.proxies = proxies
        # 设置请求头
        s.headers = headers
        # 捕获异常
        try:
            # 请求相应的 url，获取网页信息
            response = s.get(url, timeout=300)
            # 判断状态码
            if response.status_code == 200:
                # 获取网页源码
                html = response.text
                # 随机休眠 1~5s
                time.sleep(random.randint(1, 5))
                # 返回 html
                return html
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method CrawlerBase.get_page: the error of getting response: {e}")

    @staticmethod
    def get_cities(html: str) -> list:
        """
        Takes the list of cities.

        :param html: 网页源码
        :return house_city_total_list: 城市信息列表
        """
        # 获取 house_city_content_list
        house_city_content_list = re.findall(r'<span class="city-tab">(.*</a>).*?</span>', html, re.S)
        # 条件判断
        if not house_city_content_list:
            # 输出 log 信息
            print("Method CrawlerBase.get_cities: the house_city_content_list is an empty list ...")
            # 退出程序
            sys.exit(1)
        # 获取 house_city_content
        house_city_content = house_city_content_list[0]
        # 获取 house_hot_city_list
        house_hot_city_list = re.findall(r'<a href="(.*?)/" title=".*?房产网">(.*?)</a>', house_city_content, re.S)
        # 条件判断
        if not house_hot_city_list:
            # 输出 log 信息
            print("Method CrawlerBase.get_cities: the house_hot_city_list is an empty list ...")
            # 退出程序
            sys.exit(1)
        # 全部城市列表
        house_city_initial_list = re.findall(r'<a href="(.*?)" title=".*?房产网">(.*?)</a>', html, re.S)[4:]
        # 条件判断
        if not house_city_initial_list:
            # 输出 log 信息
            print("Method CrawlerBase.get_cities: house_city_initial_list is an empty list ...")
            # 退出程序
            sys.exit(1)
        # 遍历
        for url_city in house_city_initial_list:
            # 条件判断
            if url_city[1] == "北京":
                # 删除元素
                house_city_initial_list.remove(url_city)
            # 条件判断
            elif url_city[1] == "上海":
                # 删除元素
                house_city_initial_list.remove(url_city)
            # 条件判断
            elif url_city[1] == "广州":
                # 删除元素
                house_city_initial_list.remove(url_city)
            # 条件判断
            elif url_city[1] == "深圳":
                # 删除元素
                house_city_initial_list.remove(url_city)
            else:
                pass
        # 获取到最终城市列表
        house_city_total_list = house_hot_city_list + house_city_initial_list
        # 返回 house_city_total_list
        return house_city_total_list

    def max_page(self, lst: list) -> Generator:
        """
        Takes the max numbers of web pages.

        :param lst: 城市列表
        :return Generator: 生成器
        """
        # 遍历
        for url_city in lst:
            # 拼接需要的 url
            url = "https:" + url_city[0] + "/loupan/"
            # 获取相应网页源码
            html = self.get_page(url=url)
            # 条件判断
            if not html:
                # 跳入下次循环
                continue
            # 随机休眠 50~60s(防止爬虫被封)
            time.sleep(random.randint(50, 60))
            # 获取 div 标签列表
            content_list = re.findall(r'<div class="resblock-desc-wrapper">(.*?)<div class="watermark">', html, re.S)
            # 条件判断
            if not content_list:
                # 跳入下次循环
                continue
            # 获取楼盘总数列表
            data_total_count_list = re.findall(r'<div class="page-box".*?data-total-count="(.*?)">', html, re.S)
            # 条件判断
            if not data_total_count_list:
                # 跳入下次循环
                continue
            # 获取楼盘总数
            data_total_count = data_total_count_list[0]
            # 条件判断
            if not data_total_count:
                # 跳入下次循环
                continue
            # 计算最大翻页
            if int(data_total_count) % len(content_list) == 0:
                # 获取 max_page
                max_page = int(data_total_count) // len(content_list)
            else:
                # 获取 max_page
                max_page = int(data_total_count) // len(content_list) + 1
            # 生成器
            yield {
                "url": url,
                "city": url_city[1],
                "max_page": max_page
            }

    @staticmethod
    def parse_page(html: str) -> Generator:
        """
        Parse html from web pages.

        :param html: 网页源码
        :return Generator: 生成器
        """
        # 获取 div 标签列表
        content_list = re.findall(r'<div class="resblock-desc-wrapper">(.*?)<div class="watermark">', html, re.S)
        # 条件判断
        if content_list:
            # 遍历
            for item in content_list:
                # 获取 house_name_list
                house_name_list = re.findall(r'<div class="resblock-name">.*?<a href.*?>(.*?)</a>', item, re.S)
                # 条件判断
                if not house_name_list:
                    # 获取楼盘名字
                    house_name = ""
                else:
                    # 获取楼盘名字
                    house_name = house_name_list[0]
                # 获取 house_type_list
                house_type_list = re.findall(r'<span class="resblock-type".*?>(.*?)</span>', item, re.S)
                # 条件判断
                if not house_type_list:
                    # 楼盘类型
                    house_type = ""
                else:
                    # 楼盘类型
                    house_type = house_type_list[0]
                # 获取 house_status_list
                house_status_list = re.findall(r'<span class="sale-status".*?>(.*?)</span>', item, re.S)
                # 条件判断
                if not house_status_list:
                    # 楼盘情况
                    house_status = ""
                else:
                    # 楼盘情况
                    house_status = house_status_list[0]
                # 缩小范围，获取楼盘地域信息、楼盘详细地址
                house_location_content_list = re.findall(r'<div class="resblock-location">(.*?)</div>', item, re.S)
                # 条件判断
                if not house_location_content_list:
                    # 声明 house_location
                    house_location = ""
                    # 声明 house_address
                    house_address = ""
                else:
                    # 获取 house_location_content
                    house_location_content = house_location_content_list[0]
                    # 条件判断
                    if not house_location_content:
                        # 声明 house_location
                        house_location = ""
                        # 声明 house_address
                        house_address = ""
                    else:
                        # 楼盘地域列表
                        house_location_list = re.findall(r'<span>(.*?)</span>', house_location_content, re.S)
                        # 条件判断
                        if not house_location_list:
                            # 获取 house_location
                            house_location = ""
                        else:
                            # 条件判断
                            if house_location_list[0] in house_location_list[1]:
                                # 赋值
                                house_location = house_location_list[1]
                            else:
                                # 拼接
                                house_location = "".join(house_location_list)
                        # 楼盘地址列表
                        house_address_list = re.findall(r'<a href.*?>(.*?)</a>', house_location_content, re.S)
                        # 条件判断
                        if not house_address_list:
                            # 获取 house_address
                            house_address = ""
                        else:
                            # 获取 house_address
                            house_address = house_address_list[0]

                # 获取 house_room_content_list
                house_room_content_list = re.findall('<a class="resblock-room".*?>(.*?)</a>', item, re.S)
                # 条件判断
                if not house_room_content_list:
                    # 获取 house_room
                    house_room = ""
                else:
                    # 获取 house_room_content
                    house_room_content = house_room_content_list[0]
                    # 条件判断
                    if not house_room_content:
                        # 获取 house_room
                        house_room = ""
                    else:
                        # 楼盘户型列表
                        house_room_list = re.findall(r'<span>(.*?)</span>', house_room_content, re.S)
                        # 条件判断
                        if not house_room_list:
                            # 获取 house_room
                            house_room = ""
                        else:
                            # 转为字符串
                            house_room = "/".join(house_room_list)

                # 楼盘面积列表
                house_area_list = re.findall('<div class="resblock-area">.*?<span>(.*?)</span>.*?</div>', item, re.S)
                # 条件判断
                if not house_area_list:
                    # 获取 house_area
                    house_area = ""
                else:
                    # 获取 house_area
                    house_area = house_area_list[0]
                # 获取 house_tags_content_list
                house_tags_content_list = re.findall('<div class="resblock-tag">(.*?)</div>', item, re.S)
                # 条件判断
                if not house_tags_content_list:
                    # 获取 house_tags
                    house_tags = ""
                else:
                    # 获取 house_tags_content
                    house_tags_content = house_tags_content_list[0]
                    # 条件判断
                    if not house_tags_content:
                        # 获取 house_tags
                        house_tags = ""
                    else:
                        # 楼盘标签列表
                        house_tags_list = re.findall(r'<span>(.*?)</span>', house_tags_content, re.S)
                        # 条件判断
                        if not house_tags_list:
                            # 获取 house_tags
                            house_tags = ""
                        else:
                            # 获取 house_tags
                            house_tags = "/".join(house_tags_list)
                # 获取 house_content_price_list
                house_content_price_list = re.findall(r'<div class="main-price">(.*?)</div>', item, re.S)
                # 条件判断
                if not house_content_price_list:
                    # 获取 house_price
                    house_price = ""
                else:
                    house_content_price = house_content_price_list[0]
                    # 获取 house_price_list
                    house_price_list = re.findall(r'<span.*?>(.*?)</span>', house_content_price, re.S)
                    # 条件判断
                    if not house_price_list:
                        # 获取 house_price
                        house_price = ""
                    else:
                        # 字符串拼接
                        house_price = "".join(house_price_list)
                        # 条件判断
                        if "&nbsp;" in house_price:
                            # 楼盘价格数据去噪
                            house_price = house_price.replace("&nbsp;", "")
                        else:
                            pass
                # 生成器
                yield {
                    "house_name": house_name,
                    "house_type": house_type,
                    "house_status": house_status,
                    "house_location": house_location,
                    "house_address": house_address,
                    "house_room": house_room,
                    "house_area": house_area,
                    "house_tags": house_tags,
                    "house_price": house_price
                }
        else:
            # 输出 log 信息
            print("Method CrawlerBase.parse_page: content_list is an empty list ...")

    def get_lst(self, url: str) -> list:
        """
        Takes the list of result after parse html.

        :param url: 网页链接
        :return lst: 包含最终数据的列表
        """
        # 新建空列表
        lst = list()
        # 获取网页源码
        html = self.get_page(url)
        # 随机休眠 10~20s
        time.sleep(random.randint(10, 20))
        # 遍历
        for item in self.parse_page(html):
            # 条件判断
            if not item:
                # 跳入下一次循环
                continue
            # 添加元素
            lst.append(item)
        # 返回 lst
        return lst

    def main(self):
        """The method is entrance of program"""
        # 输出 log 信息
        logging.info("Method CrawlerBase.main: requesting page and Parsing html ....")
        # 开启进程池
        pool = Pool()
        # 获取网页信息
        html = self.get_page(url=self.initial_url)
        # 条件判断
        if not html:
            # 输出 log 信息
            logging.info("Method CrawlerBase.main: the html is an empty object ...")
            # 退出程序
            sys.exit(1)
        # 随机休眠 15~30s
        time.sleep(random.randint(15, 30))
        # 获取城市列表
        house_city_total_list = self.get_cities(html)
        # 遍历
        for item in self.max_page(lst=house_city_total_list):
            # 条件判断
            if not item:
                # 跳入下次循环
                continue
            # 获取 url
            url = item["url"]
            # 获取 max_page
            max_page = item["max_page"]
            # house_city
            house_city = item["city"]
            # 拼接
            url_str = url + "pg{}/"
            # 列表推导式
            result_list = [url_str.format(page) for page in range(1, max_page + 1)]
            # 遍历
            for lst in pool.map(self.get_lst, result_list):
                # 条件判断
                if not lst:
                    # 跳入下次循环
                    continue
                # 遍历
                for result in lst:
                    # 输出最终结果信息
                    print(result)
                    # 获取 house_tuple
                    house_tuple = (result["house_name"], result["house_type"], result["house_status"], house_city,
                                   result["house_location"], result["house_address"], result["house_room"],
                                   result["house_area"], result["house_tags"], result["house_price"])
                    # 列表添加元素
                    self.result_list.append(house_tuple)
        # 输出 log 信息
        logging.info("Method CrawlerBase.main: the process of Requesting and Parsing is successful!")
        # 输出 log 信息
        print("------------------------------------------------------------------------------------")
        # 输出 log 信息
        logging.info("Operating database ...")
        # 实例化 MysqlBase 对象
        mo = cs.MysqlBase(conf=self.mysql_dict)
        # 条件判断
        if mo.table_exists(self.table_name):
            # 数据插入
            mo.insert_data(self.insert_table_sql, self.result_list)
        else:
            # 新建 data sheet
            mo.create_table(self.table_name, self.create_table_sql)
            # 数据插入
            mo.insert_data(self.insert_table_sql, self.result_list)
        # 输出 log 信息
        logging.info("Method CrawlerBase.main: the process of operating database is successful!")


if __name__ == '__main__':
    # 上下文管理器
    with CrawlerBase() as cb:
        # 调用 main 方法
        cb.main()

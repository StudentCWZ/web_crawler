#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/11/8 2:58 下午
# @File: main.py


# 基本库
import logging
import os
import random
import re
import sys
import time
from typing import Generator

# 第三方库
import requests
from bs4 import BeautifulSoup
from lxml import etree

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
        # 声明 initial_url
        self.initial_url = "http://www.mafengwo.cn/mdd/"
        # # 所有代理 ip 列表
        # self.all_proxies_list = pp.ProxiesBase().all_proxies(uh.HeadersBase().get_headers())
        # # 有效代理 ip 列表
        # self.proxies_list = pp.ProxiesBase().verify_proxies(self.all_proxies_list)
        # 定义 self.mdd_list，保存目的地链接、目的地 ID 和目的地名称
        self.mdd_list = list()
        # 定义 self.result_list
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

    @staticmethod
    def get_mdd(url: str) -> Generator:
        """
        Takes the information of cities from web pages.

        :param url: 网页链接
        :return Generator: 生成器
        """
        # 请求头
        headers = {
            "Referer": "http://www.mafengwo.cn/",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # # 随机选取代理 IP
        # proxies = random.choice(self.proxies_list)
        # 初始化请求对象
        req = requests.session()
        # 设置通用 Headers
        req.headers.update(headers)
        # # 设置代理 Ip
        # req.proxies = proxies
        # 获取网页源代码
        response = req.get(url)
        # 条件判断
        if response.status_code != 200:
            # 输出 log 信息
            print("Method CrawlerBase.get_mdd: the error of requesting web page ...")
            # 退出程序
            sys.exit(1)
        # 获取网络源码
        html = response.text
        # 解析 HTMl
        soup = BeautifulSoup(html, "html.parser")
        # 休眠
        time.sleep(random.randint(20, 30))
        # 获取国内热门目的地
        hot_mdd_homeland = soup.find('div', class_='hot-list clearfix')
        # 条件判断
        if not hot_mdd_homeland:
            # 输出 log 信息
            print("Method CrawlerBase.get_mdd: the hot_mdd_homeland is an empty object ...")
            # 退出程序
            sys.exit(1)
        # 获取目的地链接
        hot_mdd_homeland_list = hot_mdd_homeland.find_all('a')
        # 条件判断
        if not hot_mdd_homeland_list:
            # 输出 log 信息
            print("Method CrawlerBase.get_mdd: the hot_mdd_homeland_list is an empty list ...")
            # 退出程序
            sys.exit(1)
        # 遍历
        for mdd in hot_mdd_homeland_list:
            # 条件判断
            if not mdd:
                # 跳入下次循环
                continue
            # 获取 link
            link = mdd["href"]
            # 获取 mdd_id
            mdd_id = re.findall(r'/travel-scenic-spot/mafengwo/(.*?).html', link)
            # 条件判断
            if not mdd_id:
                # 跳入下次循环
                continue
            # 条件判断
            if len(mdd_id) == 1 and mdd_id[0] != "":
                # 过滤部分没有 ID 的景点
                mdd_dict = {
                    "mdd_id": int(mdd_id[0]),
                    "name": mdd.text,
                    "link": "http://www.mafengwo.cn" + link
                }
                # 生成器
                yield mdd_dict
            else:
                pass

    @staticmethod
    def get_page(url: str) -> str:
        """
        Takes html from web pages.

        :param url: 网页链接
        :return html: 网页源码
        """
        # 请求头
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;'
                      'q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'piao.qunar.com',
            'User-Agent': random.choice(uh.HeadersBase().get_headers()),
            'X-Requested-With': 'XMLHttpRequest'
        }
        # 随机选取代理 IP
        # proxies = random.choice(self.proxies_list)
        # 捕获异常
        try:
            # 发起 get 请求
            response = requests.get(url=url, headers=headers, timeout=300)
            # 条件判断
            if response.status_code == 200:
                # 获取网页源码
                html = response.text
                # 休眠
                time.sleep(random.randint(20, 30))
                # 返回 html
                return html
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method CrawlerBase.get_page: the error of getting html: {e}")

    def parse_page(self, place: str) -> Generator:
        """
        Parse html.

        :param place: 地点信息
        :return Generator: 生成器
        """
        # 每个城市首页 url
        url = "http://piao.qunar.com/ticket/list.htm?keyword=" + place + "&region=&from=mpl_search_suggest&page=1"
        # 获取网页源码
        html = self.get_page(url=url)
        # 条件判断
        if html:
            # 随机休眠
            time.sleep(random.randint(50, 60))
            # 创建一个 selector 对象
            selector = etree.HTML(html)
            # 获取 max_page_list
            max_page_list = selector.xpath('//div[@class="pager"]/a[last() - 1]/text()')
            # 条件判断
            if max_page_list:
                # 获取最大翻页
                max_page = int(max_page_list[0])
                # for 循环
                for page in range(1, max_page):
                    # url
                    url = "http://piao.qunar.com/ticket/list.htm?keyword=" + place + \
                          "&region=&from=mpl_search_suggest&page={}"
                    # 获取网页源码
                    html = self.get_page(url=url.format(page))
                    # 条件判断
                    if not html:
                        # 跳入下次循环
                        continue
                    # 随机休眠
                    time.sleep(random.randint(50, 60))
                    # 创建一个 selector 对象
                    selector = etree.HTML(html)
                    # 输出 log 信息
                    print("place: ", place)
                    # 输出 log 信息
                    print("正在爬取第", str(page), "页景点信息")
                    # 获取 information
                    information = selector.xpath('//div[@class="result_list"]/div')
                    # 条件判断
                    if not information:
                        # 跳入下次循环
                        continue
                    # 遍历
                    for inf in information:
                        # 获取 sight_name_list
                        sight_name_list = inf.xpath('./div/div/h3/a/text()')
                        # 条件判断
                        if not sight_name_list:
                            # 获取 sight_name
                            sight_name = ""
                        else:
                            # 获取 sight_name
                            sight_name = sight_name_list[0]
                        # 获取 sight_level
                        sight_level = inf.xpath('.//span[@class="level"]/text()')
                        # 条件判断
                        if sight_level:
                            # 获取 sight_level
                            sight_level = sight_level[0].replace('景区', '')
                        else:
                            # 获取 sight_level
                            sight_level = "0"

                        # 获取 sight_area_list
                        sight_area_list = inf.xpath('.//span[@class="area"]/a/text()')
                        # 条件判断
                        if not sight_area_list:
                            # 获取 sight_area
                            sight_area = ""
                        else:
                            # 获取 sight_area
                            sight_area = sight_area_list[0]

                        # 获取 sight_hot_list
                        sight_hot_list = inf.xpath('.//span[@class="product_star_level"]//span/text()')
                        # 条件判断
                        if not sight_hot_list:
                            # 获取 sight_hot
                            sight_hot = ""
                        else:
                            # 获取 sight_hot
                            sight_hot = sight_hot_list[0].replace('热度 ', '')
                        # 获取 sight_add_list
                        sight_add_list = inf.xpath('.//p[@class="address color999"]/span/text()')
                        # 条件判断
                        if not sight_add_list:
                            # 获取 sight_add
                            sight_add = ""
                        else:
                            # 获取 sight_add
                            sight_add = sight_add_list[0]
                        # 获取 sight_add
                        sight_add = re.sub(r'地址：|（.*?）|\(.*?\)|，.*?$|\/.*?$', '', str(sight_add))
                        # 获取 sight_slogen_list
                        sight_slogen_list = inf.xpath('.//div[@class="intro color999"]/text()')
                        # 条件判断
                        if not sight_slogen_list:
                            # 获取 sight_slogen
                            sight_slogen = ""
                        else:
                            sight_slogen = sight_slogen_list[0].replace("\n", " ")
                        # 获取 sight_price_list
                        sight_price_list = inf.xpath('.//span[@class="sight_item_price"]/em/text()')
                        # 条件判断
                        if not sight_price_list:
                            # 获取 sight_price
                            sight_price = ""
                        else:
                            # 获取 sight_price
                            sight_price = sight_price_list[0]
                        # 获取 sight_soldnum_list
                        sight_soldnum_list = inf.xpath('.//span[@class="hot_num"]/text()')
                        # 条件判断
                        if not sight_soldnum_list:
                            # 获取 sight_soldnum
                            sight_soldnum = "0"
                        else:
                            # 获取 sight_soldnum
                            sight_soldnum = sight_soldnum_list[0]
                        # 获取 sight_point_list
                        sight_point_list = inf.xpath('./@data-point')
                        # 条件判断
                        if not sight_point_list:
                            # 获取 sight_point
                            sight_point = ""
                        else:
                            # 获取 sight_point
                            sight_point = sight_point_list[0]

                        # 获取 href_list
                        href_list = inf.xpath('.//h3/a[@class="name"]/@href')
                        # 条件判断
                        if not href_list:
                            # 获取 sight_url
                            sight_url = ""
                        else:
                            # 获取 sight_url
                            sight_url = "https://piao.qunar.com" + href_list[0]
                        # 生成器
                        yield {
                            "sight_name": sight_name,
                            "sight_level": sight_level,
                            "sight_area": sight_area,
                            "sight_price": sight_price,
                            "sight_soldnum": sight_soldnum,
                            "sight_hot": sight_hot,
                            "sight_address": sight_add.replace("地址：", ""),
                            "sight_point": sight_point,
                            "sight_slogen": sight_slogen,
                            "sight_url": sight_url
                        }
            else:
                # 输出 log 信息
                print("Method CrawlerBase.parse_page: the max_page_list is an empty list ...")
        else:
            # 输出 log 信息
            print("Method CrawlerBase.parse_page: the html is an empty object ...")

    def main(self):
        """The method is entrance of program"""
        # 输出 log 信息
        logging.info("Method CrawlerBase.main: requesting page and Parsing html ....")
        # 遍历
        for item in self.get_mdd(url=self.initial_url):
            # 条件判断
            if not item:
                # 跳入下次循环
                continue
            # 获取城市名称
            place = item["name"]
            # 遍历
            for data in self.parse_page(place):
                # 条件判断
                if not data:
                    # 跳入下次循环
                    continue
                # 输出 data
                print(data)
                # 获取 data_tuple
                data_tuple = (data.values())
                # 列表添加元素
                self.result_list.append(data_tuple)
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

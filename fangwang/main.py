#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:37 下午
# @File: main.py


# 基本库
import logging
import random
import re
import time
import urllib.request

# 第三方库
import requests
from multiprocessing import Pool
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
        # 城市列表
        self.city_tuple_list = [('广州', 'gz'), ('佛山', 'fs'), ('东莞', 'dg'), ('江西', 'jx'), ('安徽', 'ah'), ('天津', 'tj')]
        # 声明 self.url_list
        self.url_list = list()
        # 声明 self.result_list
        self.result_list = list()
        # 声明 self.mysql_dict
        self.mysql_dict = rc.ConfBase().mysql_conn_conf()
        # 声明 table_name, create_table_sql, insert_table_sql
        self.table_name, self.create_table_sql, self.insert_table_sql = rc.ConfBase().mysql_use_conf()

    @staticmethod
    def get_page(url: str) -> str:
        """:Summary: 利用 requests 发起网络请求，获取网页源码
           :Args:
                url: 网址
           :Returns
                html: 网页源码
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # 捕获异常
        try:
            # 请求相应的 url，获取网页信息
            response = requests.get(url=url, headers=headers, timeout=200)
            # 判断状态是否为 200，是否请求成功
            if response.status_code == 200:
                # 获取网页信息
                html = response.text
                # 休眠 15~20s
                time.sleep(random.randint(15, 20))
                # 返回 html
                return html
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_page: the error of getting response: {e}")

    def max_page(self, city_tuple: tuple) -> tuple:
        """
        Takes the max numbers of pages.

        :param city_tuple: 城市列表
        :return: max_page, url
        """
        # 请求参数
        param = urllib.request.quote(city_tuple[0])
        # 拼接 url
        url = r"https://{0}.ihk.cn/newhouse/houselist/?keyword={1}".format(city_tuple[1], param)
        # 获取网页源码
        html = self.get_page(url)
        # 休眠 1~3s
        time.sleep(random.randint(1, 3))
        # 捕获异常
        try:
            # 缩小网页内容
            max_page_content = re.search(r'<div class="es_conpage">(.*?)</div>', html, re.S).group(1)
            # 获取最大翻页列表
            max_page_list = re.findall(r"<a href=\"javascript:SetCondition.*?p(.*?)'", max_page_content, re.S)
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method max_page: the error of getting max_page_list: {e}")
        else:
            # 条件判断
            if max_page_list:
                # 获取 max_page
                max_page = max_page_list[-1]
            else:
                # 赋值
                max_page = 1
            # 返回 max_page, url
            return max_page, url

    @staticmethod
    def parse_page(html: str) -> Generator:
        """
        Parses the html from web pages.

        :param html: 网页源码
        :return: Generator
        """
        # 捕获异常
        try:
            # 缩小网页范围
            result_list = re.findall(r'<div class="n_conlist"(.*?), new Array', html, re.S)
            # 遍历
            for result in result_list:
                # 获取 estate_name
                estate_name = re.findall(r'<a><strong id.*?>(.*?) </strong></a>', result, re.S)[0]
                # 条件判断
                if "</b>" in estate_name:
                    # 字符替换
                    estate_name = estate_name.replace("</b>", "")
                    # 字符替换
                    estate_name = estate_name.replace('<b style="font-size:24px;color:red;font-weight:bold;">', '')
                else:
                    # 占位符
                    pass
                # 获取 estate_size_group
                estate_size_group = re.search(r'<li><i>(.*?)</i><strong>.*?</strong><b>.*?</b></li>', result, re.S)
                # 条件判断
                if estate_size_group is None:
                    # 变量赋值
                    estate_size = ""
                else:
                    # 获取 estate_size
                    estate_size = estate_size_group.group(1)
                # 获取 estate_price_group
                estate_price_group = re.search(r'<li><i>.*?</i><strong>(.*?)</strong><b>(.*?)</b></li>', result, re.S)
                # 条件判断
                if estate_price_group is None:
                    # 变量赋值
                    estate_price = ""
                else:
                    # 获取 estate_price
                    estate_price = estate_price_group.group(1) + estate_price_group.group(2)
                # 获取 estate_tag
                estate_tag = re.search(r'<div class="n_conlistrbrief">.*?<span>(.*?)</span>', result, re.S).group(1)
                # 条件判断
                if "&#183" in estate_tag:
                    # 字符替换
                    estate_tag = estate_tag.replace("&#183", "")
                else:
                    # 占位符
                    pass
                # 获取 estate_address
                estate_address = re.search(r'<div class="n_conlistradd">.*?<span>(.*?)</span></div>',
                                           result, re.S).group(1)
                # 条件判断
                if "&#183;" in estate_address:
                    # 字符替换
                    estate_address = estate_address.replace("&#183;", "")
                else:
                    # 占位符
                    pass
                # 获取 estate_status_group
                estate_status_group = re.search(r'<em class="ilp"></em><span><b>.*?</b>(.*?)</span></div>', result,
                                                re.S)
                # 条件判断
                if estate_status_group is None:
                    # 变量赋值
                    estate_status = ""
                else:
                    # 变量赋值
                    estate_status = estate_status_group.group(1)
                    # 条件判断
                    if "&#183;" in estate_status:
                        # 字符替换
                        estate_status = estate_status.replace("&#183;", "")
                    else:
                        # 占位符
                        pass
                # 获取 estate_character
                estate_character = re.search(r'var array = arraySort\(arrayStringToChar\("(.*?)"', result,
                                             re.S).group(1)
                # 生成器
                yield {
                    "estate_name": estate_name,
                    "estate_size": estate_size,
                    "estate_tag": estate_tag,
                    "estate_character": estate_character,
                    "estate_address": estate_address,
                    "estate_status": estate_status,
                    "estate_price": estate_price
                }
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method parse_page: the error of getting estate_character: {e}")

    def main(self):
        """The method is entrance of program."""
        # 开启进程池
        pool = Pool()
        # 输出 log 信息
        logging.info("Adding elements in url_list ....")
        # 遍历
        for city_tuple in self.city_tuple_list:
            # 获取最大翻页
            max_page, initial_url = self.max_page(city_tuple)
            # 休眠 8~10s
            time.sleep(random.randint(8, 10))
            # for 循环
            for page in range(1, int(max_page) + 1):
                # 拼接 url
                url = re.search(r'(https://.*?ihk.cn/newhouse/houselist/)(\?keyword=.*)',
                                initial_url, re.S).group(1) + 'p' + str(page) + '/' + \
                      re.search(r'(https://.*?ihk.cn/newhouse/houselist/)(\?keyword=.*)', initial_url, re.S).group(2)
                # 列表添加元素
                self.url_list.append(url)
        # 输出 log 信息
        logging.info("The process of getting url_list is successful!")
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # 遍历
        for html in pool.map(self.get_page, self.url_list):
            # 遍历
            for item in self.parse_page(html=html):
                # 获取 item_tuple
                item_tuple = (item["estate_name"], item["estate_size"], item["estate_tag"], item["estate_character"],
                              item["estate_address"], item["estate_status"], item["estate_price"])
                # 列表添加元素
                self.result_list.append(item_tuple)
        # 输出 log 信息
        logging.info("The process of Requesting and Parsing is successful!")
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


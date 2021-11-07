#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/11/3 9:13 上午
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

# 自定义
from conn import mongo as cm
from header import use_headers as uh
from proxies import proxies_pool as pp


class CrawlerBase(object):
    """Base class that all dedicating data implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # logging 模块初始化
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        # 定义 self.initial_url
        self.initial_url = "https://www.mafengwo.cn/mdd/"
        # 新建空列表用来接收每个城市信息
        self.cities_list = list()
        # 新建空列表用来接收每个城市的景点信息
        self.viewpoints_info = list()
        # 新建空列表用来接收每个城市的餐饮信息
        self.restaurant_info = list()
        # 新建空列表用来接收每个城市的娱乐信息
        self.entertainment_info = list()
        # 新建空列表用来接收每个城市的游记信息
        self.travel_notes_info = list()
        # 声明 self.result_list
        self.result_list = list()
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
    def get_page(url: str, proxies_list: list) -> str:
        """
         Takes the html by requesting the web page.

        :param url: 网址
        :param proxies_list: 代理 ip 列表
        :return html: 网页源码
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # 随机获取代理 IP
        proxies = random.choice(proxies_list)
        # 增加重连次数
        requests.adapters.DEFAULT_RETRIES = 5
        # 捕获异常
        try:
            # 获取网页信息
            response = requests.get(url=url, headers=headers, proxies=proxies, timeout=300)
            # 判断状态码
            if response.status_code == 200:
                # 获取网页源码
                html = response.text
                # 休眠 10~15s
                time.sleep(random.randint(15, 20))
                # 返回 html
                return html
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_page: the error of getting response: {e}")

    @staticmethod
    def get_cities(html: str) -> list:
        """
        Takes cities list from the web page.

        :param html: 网页源码
        :return cities_list: 城市列表
        """
        # 声明 cities_list
        cities_list = list()
        # 捕获异常
        try:
            # 获取 div 标签
            div_html = re.findall(r'<div class="row row-hot">(.*?)<div class="row-line">', html, re.S)[0]
            # 获取 dd 标签列表
            dd_tags_list = re.findall(r'<dd>(.*?)</dd>', div_html, re.S)
            # 遍历
            for dd_html in dd_tags_list:
                # 获取 hot_cities 列表
                city_list = re.findall('<a href="(.*?)" target="_blank">(.*?)</a>', dd_html, re.S)
                # 列表拼接
                cities_list += city_list
            # 返回 cities_list
            return cities_list
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_cities: the error of getting cities_list: {e}")

    @staticmethod
    def parse_cities_page(html: str, city_id: str) -> Generator:
        """
        Parse html from the web page about cities.

        :param html: 网页源码
        :param city_id: 城市对应 id
        :return Generator: 生成器
        """
        # 获取 div 标签
        div_html_list = re.findall(r'<div class="navbar-con">(.*?)<div class="overview-drop hide" data-cs-p="概况菜单">',
                                   str(html), re.S)
        # 条件判断
        if div_html_list:
            #  获取 div_html
            div_html = div_html_list[0]
            # 获取 a 标签
            a_html_list = re.findall(r'href="(.*?)".*?data-cs-p="(.*?)"', div_html, re.S)
            # 条件判断
            if a_html_list:
                # 遍历
                for classification_href, classification_name in a_html_list:
                    # 条件判断
                    if classification_href == "" and classification_name == "旅游度假":
                        # format 操作
                        classification_href = "/sales/0-0-M{}-0-0-0-0-0.html".format(city_id)
                    elif classification_href == "" and classification_name == "游记":
                        # format 操作
                        classification_href = "/yj/{}/".format(city_id)
                    elif classification_href == "" and classification_name == "餐饮":
                        # format 操作
                        classification_href = "/cy/{}/gonglve.html".format(city_id)
                    elif classification_href == "" and classification_name == "地图":
                        # format 操作
                        classification_href = "/mdd/map/{}.html".format(city_id)
                    else:
                        pass
                    # 条件判断
                    if classification_href:
                        # url 拼接
                        classification_url = "https://www.mafengwo.cn" + classification_href
                        # 获取 classification_tuple
                        classification_tuple = (classification_url, classification_name)
                        # generator
                        yield classification_tuple
                    else:
                        # 跳入下次循环
                        continue
            else:
                # 输出 log 信息
                print("Method parse_cities_page: a_html_list is an empty list ...")
        else:
            # 输出 log 信息
            print("Method parse_cities_page: div_html_list is an empty list ...")

    @staticmethod
    def parse_viewpoint_page(html: str) -> Generator:
        """
        Parse html from the web page about viewpoint.

        :param html: 网页源码
        :return Generator: 生成器
        """
        # 获取 div 标签
        div_html_list = re.findall(r'<div class="middle">(.*?</p>)', html, re.S)
        # 条件判断
        if div_html_list:
            # 遍历
            for div_html in div_html_list:
                # 条件判断
                if "<span" in div_html:
                    # 获取 viewpoints_name
                    viewpoints_name = re.findall(r'<a href=".*?" target="_blank".*?title="(.*?)">', div_html, re.S)[0]
                    # 获取 viewpoints_synopsis
                    viewpoints_synopsis = re.findall(r'<p>(.*?)</p>', div_html, re.S)[0]
                else:
                    # 获取 viewpoints_name
                    viewpoints_name = re.findall(r'<h3>(.*?)</h3>', div_html, re.S)[0]
                    # 获取 viewpoints_synopsis
                    viewpoints_synopsis = re.findall(r'<p>(.*?)</p>', div_html, re.S)[0]
                # 生成器
                yield {
                    "viewpoints_name": viewpoints_name,
                    "viewpoints_synopsis": viewpoints_synopsis
                }
        else:
            # 输出 log 信息
            print("Method parse_viewpoint_page: div_html_list is an empty list ...")

    def parse_repast_page(self, html: str, url: str, proxies_list: list) -> Generator:
        """
        Parse html the web page about repast.

        :param html: 网页源码
        :param url: 网页链接
        :param proxies_list: 代理 ip 列表
        :return Generator: 生成器
        """
        # 获取 max_page_list
        max_page_list = re.findall(r'<span class="count">.*?<span>(.*?)</span>', html, re.S)
        # 条件判断
        if max_page_list:
            # 获取最大翻页
            max_page_num = max_page_list[0]
            # for 循环
            for i in range(1, int(max_page_num) + 1):
                # url 拼接
                start_url = url.replace("gonglve.html", "") + '0-0-0-0-0-{}.html'
                # format 操作
                final_url = start_url.format(i)
                # 获取网页源码
                html = self.get_page(url=final_url, proxies_list=proxies_list)
                # 条件判断
                if not html:
                    # 跳入下次循环
                    continue
                # 休眠 3~5s
                time.sleep(random.randint(3, 5))
                # 获取 li 标签
                li_html_list = re.findall(r'<li class="item clearfix">(.*?)</li>', html, re.S)
                # 条件判断
                if not li_html_list:
                    # 跳入下次循环
                    continue
                # 遍历
                for li_html in li_html_list:
                    # 获取 restaurant_name_list
                    restaurant_name_list = re.findall(r'<a href=".*?" target="_blank" >(.*?)</a>', li_html, re.S)
                    # 条件判断
                    if not restaurant_name_list:
                        # 跳入下次循环
                        continue
                    # 获取 restaurant_name
                    restaurant_name = restaurant_name_list[0]
                    # 获取 restaurant_comment_list
                    restaurant_comment_list = re.findall(r'<p class="rev-num"><em>(.*?)</em>.*?</p>', li_html, re.S)
                    # 条件判断
                    if not restaurant_comment_list:
                        # 跳入下次循环
                        continue
                    # 获取 restaurant_comment_num
                    restaurant_comment_num = restaurant_comment_list[0]
                    # 生成器
                    yield {
                        "restaurant_comment": restaurant_name,
                        "restaurant_comment_num": restaurant_comment_num
                    }
        else:
            # 输出 log 信息
            print("Method parse_repast_page: max_page_list is an empty list ...")

    def parse_entertainment_page(self, html: str, url: str, proxies_list: list) -> Generator:
        """
        Parse html the web page about entertainment.

        :param html: 网页源码
        :param url: 网页链接
        :param proxies_list: 代理 ip 列表
        :return Generator: 生成器
        """
        # 获取 max_page_list
        max_page_list = re.findall(r'<span class="count">.*?<span>(.*?)</span>', html, re.S)
        # 条件判断
        if max_page_list:
            # 获取最大翻页
            max_page_num = max_page_list[0]
            # for 循环
            for i in range(1, int(max_page_num) + 1):
                # 拼接 url
                start_url = url.replace("gonglve.html", "") + '0-0-0-0-0-{}.html'
                # 获取 final_url
                final_url = start_url.format(i)
                # 获取网页源码
                html = self.get_page(url=final_url, proxies_list=proxies_list)
                # 条件判断
                if not html:
                    # 跳入下次循环
                    continue
                # 休眠 3~5s
                time.sleep(random.randint(3, 5))
                # 获取 li 标签
                li_html_list = re.findall(r'<li class="item clearfix">(.*?)</li>', html, re.S)
                # 条件判断
                if not li_html_list:
                    # 跳入下次循环
                    continue
                # 遍历
                for li_html in li_html_list:
                    # 获取 place_list
                    place_list = re.findall(r'<a href=".*?" target="_blank" >(.*?)</a>', li_html, re.S)
                    # 条件判断
                    if not place_list:
                        # 跳入下次循环
                        continue
                    # 获取 place_name
                    place_name = place_list[0]
                    # 获取 place_comment_list
                    place_comment_list = re.findall(r'<p class="rev-num"><em>(.*?)</em>.*?</p>', li_html, re.S)
                    # 条件判断
                    if not place_comment_list:
                        # 跳入下次循环
                        continue
                    # 获取 place_comment_num
                    place_comment_num = place_comment_list[0]
                    # 生成器
                    yield {
                        "place_name": place_name,
                        "place_comment_num": place_comment_num
                    }
        else:
            # 输出 log 信息
            print("Method parse_entertainment_page: max_page_list is an empty list ...")

    def parse_travelnotes_page(self, html: str, url: str, proxies_list: list) -> Generator:
        """
        Parse html the web page about travelnotes.

        :param html: 网页源码
        :param url: 网页链接
        :param proxies_list: 代理 ip 列表
        :return Generator: 生成器
        """
        # 获取 max_page_list
        max_page_list = re.findall(r'<span class="count">.*?<span>(.*?)</span>', html, re.S)
        # 条件判断
        if max_page_list:
            # 获取最大翻页
            max_page_num = max_page_list[0]
            # 遍历
            for i in range(1, int(max_page_num) + 1):
                # url 拼接
                start_url = url + '1-0-{}.html'
                # url 格式化
                final_url = start_url.format(i)
                # 获取网页源码
                html = self.get_page(url=final_url, proxies_list=proxies_list)
                # 条件判断
                if not html:
                    # 跳入下次循环
                    continue
                # 休眠 3~5s
                time.sleep(random.randint(3, 5))
                # 获取 li 标签
                li_html_list = re.findall(r'<li class="post-item clearfix">(.*?)</li>', html, re.S)
                # 条件判断
                if not li_html_list:
                    # 跳入下次循环
                    continue
                # 遍历
                for li_html in li_html_list:
                    # 获取 travel_notes_title_list
                    travel_notes_title_list = re.findall(r'<a href=".*?" class="title-link" target="_blank">(.*?)</a>',
                                                         li_html, re.S)
                    # 条件判断
                    if not travel_notes_title_list:
                        # 跳入下次循环
                        continue
                    # 获取 travel_notes_title
                    travel_notes_title = travel_notes_title_list[0].replace("\n", "")
                    # 获取 travel_notes_author_list
                    travel_notes_author_list = re.findall(r'<span class="author">.*?作者.*?>(.*?)'
                                                          r'</a>.*?<span class="last-comment">', li_html, re.S)
                    # 条件判断
                    if not travel_notes_author_list:
                        # 跳入下次循环
                        continue
                    # 获取 travel_notes_author
                    travel_notes_author = travel_notes_author_list[0]
                    # 获取 travel_notes_date_list
                    travel_notes_date_list = re.findall(r'<span class="comment-date">(.*?)</span>', li_html, re.S)
                    # 条件判断
                    if not travel_notes_date_list:
                        # 跳入下次循环
                        continue
                    # 获取 travel_notes_date
                    travel_notes_date = travel_notes_date_list[0]
                    # 获取 travel_notes_content_list
                    travel_notes_content_list = re.findall(r'<div class="post-content">(.*?)</div>', li_html, re.S)
                    # 条件判断
                    if not travel_notes_content_list:
                        # 跳入下次循环
                        continue
                    # 获取 travel_notes_content
                    travel_notes_content = travel_notes_content_list[0].strip().replace("\n", "").\
                        replace("\u3000", "").replace("\u200d", "").replace("xa0", "").replace("\xa0", "")
                    # 生成器
                    yield {
                        "travel_notes_title": travel_notes_title,
                        "travel_notes_author": travel_notes_author,
                        "travel_notes_date": travel_notes_date,
                        "travel_notes_content": travel_notes_content
                    }
        else:
            # 输出 log 信息
            print("Method parse_travelnotes_page: max_page_list is an empty list ...")

    def main(self):
        """The method is entrance of program"""
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
        # 获取网页源码
        html = self.get_page(url=self.initial_url, proxies_list=proxies_list)
        # 条件判断
        if not html:
            # 输出 log 信息
            print("Method main: html is an empty object ...")
            # 程序异常：退出程序
            sys.exit(1)
        # 获取 hot_cities 列表
        city_tuple_list = self.get_cities(html=html)
        # 条件判断
        if not city_tuple_list:
            # 输出 log 信息
            print("Method main: city_tuple_list is an empty list ...")
            # 程序异常：退出程序
            sys.exit(1)
        # 遍历
        for city_tuple in city_tuple_list:
            # 赋值
            city_href, city_name = city_tuple
            # 获取 city_id_list
            city_id_list = re.findall(r'/travel-scenic-spot/mafengwo/(.*?).html', city_href, re.S)
            # 条件判断
            if not city_id_list:
                # 跳入下次循环
                continue
            # 获取 city_id
            city_id = city_id_list[0]
            # 获取 city_url
            city_url = "https://www.mafengwo.cn" + city_href
            # 获取 city_html
            city_html = self.get_page(url=city_url, proxies_list=proxies_list)
            # 条件判断
            if not city_html:
                # 跳入下次循环
                continue
            # 休眠 5~8s
            time.sleep(random.randint(5, 8))
            # 遍历
            for classification_tuple in self.parse_cities_page(city_html, city_id):
                # 条件判断
                if not classification_tuple:
                    # 跳入下次循环
                    continue
                # 赋值
                classification_url, classification_name = classification_tuple
                # 条件判断
                if classification_name == "景点":
                    # 获取 classification_html
                    classification_html = self.get_page(url=classification_url, proxies_list=proxies_list)
                    # 条件判断
                    if not classification_html:
                        # 跳入下次循环
                        continue
                    # 休眠 1~3s
                    time.sleep(random.randint(1, 3))
                    # 遍历
                    for viewpoints_item in self.parse_viewpoint_page(classification_html):
                        # 条件判断
                        if not viewpoints_item:
                            # 跳入下次循环
                            continue
                        # 输出 log 信息
                        print(viewpoints_item)
                        # 列表添加元素
                        self.viewpoints_info.append(viewpoints_item)
                elif classification_name == "餐饮":
                    # 获取 classification_html
                    classification_html = self.get_page(url=classification_url, proxies_list=proxies_list)
                    # 条件判断
                    if not classification_html:
                        # 跳入下次循环
                        continue
                    # 休眠 1~3s
                    time.sleep(random.randint(1, 3))
                    # 捕获异常
                    try:
                        # 遍历
                        for restaurant_item in self.parse_repast_page(html=classification_html, url=classification_url,
                                                                      proxies_list=proxies_list):
                            # 输出 log 信息
                            print(restaurant_item)
                            # 列表添加元素
                            self.restaurant_info.append(restaurant_item)
                    except Exception as e:
                        # 输出 log 信息
                        logging.error(f"Method main: the error of getting restaurant_item : {e}")
                elif classification_name == "娱乐":
                    # 获取 classification_html
                    classification_html = self.get_page(url=classification_url, proxies_list=proxies_list)
                    # 条件判断
                    if not classification_html:
                        # 跳入下次循环
                        continue
                    # 休眠 1~3s
                    time.sleep(random.randint(1, 3))
                    # 捕获异常
                    try:
                        # 遍历
                        for Entertainment_item in self.parse_entertainment_page(html=classification_html,
                                                                                url=classification_url,
                                                                                proxies_list=proxies_list):
                            # 输出 log 信息
                            print(Entertainment_item)
                            # 列表添加元素
                            self.entertainment_info.append(Entertainment_item)
                    except Exception as e:
                        # 输出 log 信息
                        logging.error(f"Method main: the error of Entertainment_item : {e}")
                elif classification_name == "游记":
                    # 获取 classification_html
                    classification_html = self.get_page(url=classification_url, proxies_list=proxies_list)
                    # 条件判断
                    if not classification_html:
                        # 跳入下次循环
                        continue
                    # 休眠 1~3s
                    time.sleep(random.randint(1, 3))
                    # 捕获异常
                    try:
                        # 遍历
                        for travel_notes_item in self.parse_travelnotes_page(html=classification_html,
                                                                             url=classification_url,
                                                                             proxies_list=proxies_list):
                            # 输出 log 信息
                            print(travel_notes_item)
                            # 列表添加元素
                            self.travel_notes_info.append(travel_notes_item)
                    except Exception as e:
                        # 输出 log 信息
                        logging.error(f"Method main: the error of getting travel_notes_item: {e}")
                else:
                    pass

            # 获取 city_info
            city_info = {
                "city_name": city_name,
                "viewpoints_info": self.viewpoints_info,
                "restaurant_info": self.restaurant_info,
                "entertainment_info": self.entertainment_info,
                "travel_notes_info": self.travel_notes_info
            }
            # 输出 city_info
            print(city_info)
            # 列表添加元素
            self.result_list.append(city_info)
        # 输出 log 信息
        logging.info("The process of Requesting and Parsing is successful!")
        # 输出 log 信息
        print("------------------------------------------------------------------------------------")
        # 输出 log 信息
        logging.info("Operating database ...")
        # 实例化 connMongo 对象，用于后续的数据插入 mongo
        mb = cm.MongoBase(db_name="spider", collection_name="mafengwo")
        # 条件判断
        if mb.add_many(infos=self.result_list):
            # 输出 log 信息
            logging.info("The process of inserting data is successful!")
        else:
            # 输出 log 信息
            logging.info("The process of inserting data is failed!")
        # 输出 log 信息
        logging.info("The process of operating database is successful!")


if __name__ == "__main__":
    # 上下文管理器
    with CrawlerBase() as cb:
        # 调用 main 方法
        cb.main()


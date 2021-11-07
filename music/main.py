#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/11/3 2:56 下午
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
from typing import Generator

# 自定义
from conf import read_conf as rc
from conn import mongo as cm
from header import use_headers as uh
from proxies import proxies_pool as pp


class CrawlerBase(object):
    """Base class that all dedicating data implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # logging 模块初始化
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        # 初始 url
        self.initial_url = rc.ConfBase().url_conf()
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
    def get_page(url: str, proxies_list: list) -> str:
        """
        Takes the html by requesting the web page.


        :param url: 网址
        :param proxies_list: 代理 ip 列表
        :return: html: 网页源码
        """
        # 随机构造请求头
        headers = {
            "User-Agent": random.choice(uh.HeadersBase().get_headers())
        }
        # 随机获取代理 IP
        proxies = random.choice(proxies_list)
        # 增加重连次数
        requests.adapters.DEFAULT_RETRIES = 5
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
            # 获取网页信息
            response = s.get(url=url, timeout=100)
            # 条件判断
            if response.status_code == 200:
                # 获取网页源码
                html = response.text
                # 随机休眠 20~25s
                time.sleep(random.randint(20, 25))
                # 返回 html
                return html
        except Exception as e:
            # 输出 log 信息
            logging.error(f"Method get_page: the error of getting response: {e}")

    @staticmethod
    def parse_main_page(html: str) -> list:
        """
        Parse html from the web page: https://music.163.com/discover/artist.

        :param html: 网页源码
        :return classification_list: 歌手分类列表
        """
        # 新建列表，接收解析信息
        classification_list = list()
        # 获取 div 标签列表
        div_html_list = re.findall(r'(<div class="blk">.*?</div>)', html, re.S)
        # 条件判断
        if div_html_list:
            # 遍历
            for div_html in div_html_list:
                # 获取 li 标签列表
                li_html_list = re.findall(r'<li><a href="(.*?)".*?data-cat=".*?">(.*?)</a>.*?</li>', div_html, re.S)
                # 条件判断
                if not li_html_list:
                    # 跳入下个循环
                    continue
                # 遍历
                for classification_href, artist_classification in li_html_list:
                    # url 拼接
                    classification_url = "https://music.163.com" + classification_href
                    # 获取 classification_tuple
                    classification_tuple = (classification_url, artist_classification)
                    # 列表添加元素
                    classification_list.append(classification_tuple)
            # 返回 classification_list
            return classification_list
        else:
            # 输出 log 信息
            print("Method parse_main_page: div_html_list is an empty list ...")

    @staticmethod
    def parse_singer_page(html: str) -> Generator:
        """
        Parse html from the web page about singer.

        :param html: 网页源码
        :return Generator: 生成器
        """
        # 获取 div 标签列表
        div_html_list = re.findall(r'<div class="u-cover u-cover-5">(.*?)</div>', html, re.S)
        # 条件判断
        if div_html_list:
            # 遍历
            for div_html in div_html_list:
                # 获取 singer_list
                singer_list = re.findall(r'<a title="(.*?)" href="(.*?)" class="msk"></a>', div_html, re.S)
                # 条件判断
                if not singer_list:
                    # 跳入下个循环
                    continue
                # 获取 singer_name, singer_href
                singer_name, singer_href = singer_list[0]
                # 获取 singer_url, singer_name
                singer_url = "https://music.163.com" + singer_href
                # 切片
                singer_name = singer_name[:-3]
                # 获取 singer_tuple
                singer_tuple = (singer_name, singer_url)
                # generator
                yield singer_tuple
        else:
            # 输出 log 信息
            print("Method parse_singer_page: div_html_list is an empty list ...")
        # li 标签列表
        li_html_list = re.findall(r'<li class="sml">(.*?)</li>', html, re.S)
        # 条件判断
        if li_html_list:
            # 遍历
            for li_html in li_html_list:
                # 获取 singer_list
                singer_list = re.findall(r'<a href="(.*?)".*?>(.*?)</a>', li_html, re.S)
                # 条件判断
                if not singer_list:
                    # 跳入下个循环
                    continue
                # 获取 singer_name, singer_href
                singer_name, singer_href = singer_list[0]
                # 获取 singer_url
                singer_url = "https://music.163.com" + singer_href
                # 获取 singer_tuple
                singer_tuple = (singer_name, singer_url)
                # generator
                yield singer_tuple
        else:
            # 输出 log 信息
            print("Method parse_singer_page: li_html_list is an empty list ...")

    @staticmethod
    def parse_song_page(html: str) -> Generator:
        """
        Parse html from the web page about songs.

        :param html: 网页源码
        :return Generator: 生成器
        """
        # div 标签列表
        hot_song_html_list = re.findall(r'<div id="hotsong-list">.*?<ul class="f-hide">(.*?)</ul>', html, re.S)
        # 条件判断
        if hot_song_html_list:
            # div 标签
            hot_song_html = hot_song_html_list[0]
            # 获取 hot_song_list
            hot_song_list = re.findall(r'<li><a href="(.*?)">(.*?)</a></li>', hot_song_html, re.S)
            # 条件判断
            if hot_song_list:
                # 遍历
                for hot_song in hot_song_list:
                    # 获取 song_href, song_name
                    song_href, song_name = hot_song
                    # 条件判断
                    if "歌曲：" in song_name:
                        song_name = song_name.replace("歌曲：", "")
                    else:
                        # 占位符
                        pass
                    # 获取 song_url
                    song_url = "https://music.163.com" + song_href
                    # 获取 song_tuple
                    song_tuple = song_name, song_url
                    # generator
                    yield song_tuple
            else:
                # 输出 log 信息
                print("Method parse_song_page: hot_song_list is an empty list ...")
        else:
            # 输出 log 信息
            print("Method parse_song_page: hot_song_html_list is an empty list ...")

    def main(self):
        """The method is entrance of program"""
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # 所有代理 ip 列表
        all_proxies_list = pp.ProxiesBase().all_proxies(uh.HeadersBase().get_headers())
        # 有效代理 ip 列表
        proxies_list = pp.ProxiesBase().verify_proxies(all_proxies_list)
        # 获取网页源码
        main_html = self.get_page(url=self.initial_url, proxies_list=proxies_list)
        # 条件判断
        if not main_html:
            # 输出 log 信息
            print("Method main: the main_html is an empty object ....")
            # 程序异常: 退出程序
            sys.exit(1)
        # 解析网页
        classification_list = self.parse_main_page(main_html)
        # 条件判断
        if not classification_list:
            # 输出 log 信息
            print("Method main: classification_list is an empty object ....")
            # 程序异常: 退出程序
            sys.exit(1)
        # 遍历
        for classification_url, artist_classification in classification_list:
            # 获取网页源码
            singer_html = self.get_page(classification_url, proxies_list)
            # 条件判断
            if not singer_html:
                # 跳入下次循环
                continue
            # 获取 singer_tuple
            singer_tuple = self.parse_singer_page(singer_html)
            # 条件判断
            if not singer_tuple:
                # 跳入下次循环
                continue
            # 遍历
            for singer_name, singer_url in singer_tuple:
                # 获取网页源码
                song_html = self.get_page(singer_url, proxies_list)
                # 条件判断
                if not song_html:
                    # 跳入下次循环
                    continue
                # 解析网页
                song_tuple = self.parse_song_page(song_html)
                # 条件判断
                if not song_tuple:
                    # 跳入下次循环
                    continue
                # 遍历
                for song_name, song_url in song_tuple:
                    # 输出 log 信息
                    print("artist_classification:", artist_classification)
                    # 输出 log 信息
                    print("singer_name:", singer_name)
                    # 输出 log 信息
                    print("song_name: ", song_name)
                    # 获取网页源码
                    details_html = self.get_page(song_url, proxies_list)
                    # 条件判断
                    if not details_html:
                        # 跳入下次循环
                        continue
                    # 获取 album_list
                    album_list = re.findall(r'<p class="des s-fc4">.*?<a href="(.*?)".*?>(.*?)</a></p>',
                                            details_html, re.S)
                    # 条件判断
                    if not album_list:
                        # 跳入下次循环
                        continue
                    # 获取 album_href, album_name
                    album_href, album_name = album_list[0]
                    # 条件判断
                    if "<" in album_name:
                        # 变量赋值
                        album_name = ""
                        # 变量赋值
                        album_url = ""
                    else:
                        # 获取 album_url
                        album_url = "https://music.163.com" + album_href
                    # 获取 song_href_list
                    song_href_list = re.findall(r'<a data-action="outchain".*?data-href="(.*?)".*?>.*?</a>',
                                                details_html, re.S)
                    # 条件判断
                    if not song_href_list:
                        # 跳入下次循环
                        continue
                    # 获取 song_href
                    song_href = song_href_list[0]
                    # 拼接 url
                    song_out_url = "https://music.163.com" + song_href
                    # 输出最终结果
                    print(artist_classification, singer_name, song_name, album_name, singer_url, song_url,
                          song_out_url, album_url)
                    # 获取 item_tuple
                    '''
                    item_tuple = (artist_classification, singer_name, song_name, album_name, singer_url, song_url,
                                  song_out_url, album_url)
                    '''
                    # 获取 item_dict
                    item_dict = {
                        "artist_classification": artist_classification,
                        "singer_name": singer_name,
                        "song_name": song_name,
                        "album_name": album_name,
                        "singer_url": singer_url,
                        "song_url": song_url,
                        "song_out_url": song_out_url,
                        "album_url": album_url
                    }
                    # 列表添加元素
                    self.result_list.append(item_dict)
        # 输出 log 信息
        logging.info("The process of Requesting and Parsing is successful!")
        # 输出 log 信息
        print("------------------------------------------------------------------------------------")
        # 输出 log 信息
        logging.info("Operating database ...")
        # 实例化 connMongo 对象，用于后续的数据插入 mongo
        mb = cm.MongoBase(db_name="spider", collection_name="music_test_1")
        # 条件判断
        if mb.add_many(infos=self.result_list):
            # 输出 log 信息
            logging.info("The process of inserting data is successful!")
        else:
            # 输出 log 信息
            logging.info("The process of inserting data is failed!")
        # 输出 log 信息
        logging.info("The process of operating database is successful!")


if __name__ == '__main__':
    # 上下文管理器
    with CrawlerBase() as cb:
        # 调用 main 方法
        cb.main()

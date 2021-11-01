#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 3:59 下午
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

# 自定义
from header import use_headers as uh


class CrawlerBase(object):
    """Base class that all dedicating data implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # logging 模块初始化
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
        # 声明 self.initial_url
        self.initial_url = r'https://www.qiushibaike.com/8hr/page/2/'

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
        # 获取 li 标签
        result = re.findall(r'(<li class="item.*?>.*?</li>)', html, re.S)
        # 条件判断
        if not result:
            # 输出 debug 信息
            print("Method parse_page: the error of getting result ...")
        # 遍历
        for content in result:
            # 获取图片/视频链接
            picture_content_list = re.findall(r'<a class="recmd.*?<img src="(.*?) alt=.*?</a>', content, re.S)
            # 条件判断
            if not picture_content_list:
                # 声明 picture_url
                picture_url = ""
            else:
                # 拼接 url
                picture_url = "https:" + picture_content_list[0]
            # 获取文章内容
            article_content_list = re.findall(r'<a class="recmd-content".*?>(.*?)</a>', content, re.S)
            # 条件判断
            if not article_content_list:
                # 声明 article_content
                article_content = ""
            else:
                # 获取 article_content
                article_content = article_content_list[0]
                # 条件判断
                if "<img" in article_content:
                    article_content = re.findall(r'(.*?)<img.*">', article_content, re.S)[0]
                else:
                    pass
            # 获取昵称
            article_user_list = re.findall(r'<span class="recmd-name">(.*?)</span>', content, re.S)
            # 条件判断
            if not article_user_list:
                # 声明 article_user
                article_user = ""
            else:
                # 获取 article_user
                article_user = article_user_list[0]
            # 缩小范围
            mini_content_list = re.findall(r'<div class="recmd-num">(.*)</div>', content, re.S)
            # 条件判断
            if not mini_content_list:
                # 输出 debug 信息
                print("Method parse_page: the error of getting mini_content_list ...")
                # 声明 happy_num
                happy_num = 0
                # 声明 command_num
                command_num = 0
            else:
                # 获取 mini_content
                mini_content = mini_content_list[0]
                # 获取 span_list
                span_list = re.findall(r'<span>(.*?)</span>', mini_content, re.S)
                # 条件判断
                if not span_list:
                    # 输出 debug 信息
                    print("Method parse_page: the error of getting span_list ...")
                    # 声明 happy_num
                    happy_num = 0
                    # 声明 command_num
                    command_num = 0
                else:
                    # 条件判断
                    if len(span_list) == 2:
                        # 获取 happy_num
                        happy_num = int(span_list[0])
                        # 获取 command_num
                        command_num = 0
                    elif len(span_list) == 5:
                        # 获取 happy_num
                        happy_num = int(span_list[0])
                        # 获取 command_num
                        command_num = int(span_list[-2])
                    else:
                        # 声明 happy_num
                        happy_num = 0
                        # 声明 command_num
                        command_num = 0
            # 生成器
            yield {
                "picture_url": picture_url,
                "article_content": article_content,
                "article_user": article_user,
                "happy_num": happy_num,
                "command_num": command_num
            }

    def main(self):
        """The method is entrance of program."""
        # 输出 log 信息
        print("################################ Running main.py #################################")
        # 输出 log 信息
        logging.info("Requesting page and Parsing html ....")
        # 获取原始 url 对应的网页信息
        html = self.get_page(url=self.initial_url)
        # 获取最大翻页列表
        max_page_list = re.findall(r'<span class="page-numbers">(.*?)</span>', html, re.S)
        # 条件判断
        if not max_page_list:
            # 输出 debug 信息
            print("Method main: the error of getting max_page_list ...")
            # 程序异常，退出程序
            sys.exit(1)
        else:
            max_page = max_page_list[-1]
        # 遍历 url，获取所有页面信息
        for i in range(0, int(max_page)):
            # 拼接 url
            url = "https://www.qiushibaike.com/8hr/page/{}".format(i + 1)
            # 输出 log 信息
            print("requesting url: %s" % url)
            # 获取网页源码
            html = self.get_page(url)
            # 条件判断
            if not html:
                # 跳入下个循环
                continue
            # 休眠 1~3s
            time.sleep(random.randint(1, 3))
            # 遍历
            for article in self.parse_page(html):
                # 条件判断
                if not article:
                    # 跳入下个循环
                    continue
                # 输出最终信息
                print(article)
        # 输出 log 信息
        logging.info("The process of Requesting and Parsing is successful!")
        # 输出 log 信息
        print("################################ Exiting main.py #################################")


if __name__ == '__main__':
    # 实例化 CrawlerBase 对象
    cb = CrawlerBase()
    # 调用 main 函数
    cb.main()


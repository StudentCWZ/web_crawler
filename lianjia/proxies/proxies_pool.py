#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/11/8 10:28 上午
# @File: proxies_pool.py


# 基本库
import os
import random
import re
import sys
import time

# 第三方库
import requests
from typing import Generator


class ProxiesBase(object):
    """Base class that all proxies implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # 声明 self.initial_url
        self.initial_url = "https://www.kuaidaili.com/free/"
        # 获取 self.dir_path
        self.dir_path = os.path.split(os.path.realpath(__file__))[0]
        # 获取 self.file_path
        self.file_path = os.path.join(self.dir_path, "all_proxies.txt")

    def read_txt(self) -> list:
        """
        Takes ip data from local file.

        :return proxies_list: 代理 ip 池
        """
        # 声明一个空列表
        proxies_list = list()
        # 打开文件
        with open(self.file_path, "r", encoding="utf-8") as f:
            # 读取文件
            lst = [item.strip() for item in f.readlines()]
            # 遍历
            for item in lst:
                # 获取 ip_type
                ip_type = item.split(':')[0].replace('"', '').replace('{', '')
                # 获取 ip
                ip = item.split(':')[1][2:]
                # 获取 port
                port = item.split(':')[2].replace('"', '').replace('}', '')
                # 获取 ip_dict
                ip_dict = {
                    "ip_type": ip_type,
                    "ip": ip,
                    "port": port
                }
                # 列表添加元素
                proxies_list.append(ip_dict)
            # 文件关闭
            f.close()
        # 返回 proxies_list
        return proxies_list

    def max_page(self, header_list: list) -> str:
        """
        Get max pages from https://www.kuaidaili.com/free/

        :param header_list: 请求头列表
        :return max_page: 最大翻页
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(header_list)
        }
        # 捕获异常
        try:
            # 发起 get 请求
            response = requests.get(url=self.initial_url, headers=headers, timeout=60)
            # 获取网页源码
            html = response.text
        except Exception as e:
            # 输出 log 信息
            print(f"Method max_page: the error of getting web page: {e}")
            # 声明 html
            html = ""
        # 条件判断
        if not html:
            # 输出 debug 信息
            print("Method max_page: the error of getting html ...")
            # 程序异常，退出程序
            sys.exit(1)
        # 正则表达式
        div_html = re.findall(r'<div id="listnav">(.*?)</div>', html, re.S)[0]
        # 条件判断
        if not div_html:
            # 输出 debug 信息
            print(f"Method max_page: the error of getting div_html ...")
            # 程序异常，退出程序
            sys.exit(1)
        # 正则表达式
        max_page = re.findall(r'<a.*?>(.*?)</a>', div_html, re.S)[-1]
        # 条件判断
        if not max_page:
            # 输出 debug 信息
            print(f"Method max_page: the error of getting max_page ...")
            # 程序异常，退出程序
            sys.exit(1)
        # 返回 max_page
        return max_page

    @staticmethod
    def get_page(url: str, header_list: list) -> str:
        """
        Takes html of web pages.

        :param url: 网址
        :param header_list: 请求头列表
        :return html: 网页源码
        """
        # 构造请求头
        headers = {
            "User-Agent": random.choice(header_list)
        }
        # 捕获异常
        try:
            # 发起 get 请求
            response = requests.get(url=url, headers=headers, timeout=60)
            # 判断状态是否为 200，是否请求成功
            if response.status_code == 200:
                # 获取网页信息
                html = response.text
                # 休眠
                time.sleep(random.randint(3, 5))
            else:
                # 声明 html
                html = ""
            # 返回 html
            return html
        except Exception as e:
            # 输出 log 信息
            print(f"Method get_page: the error of getting web page: {e}")

    @staticmethod
    def get_proxy(html: str) -> Generator:
        """
        Takes proxies by parsing html.

        :param html: 网页源码
        :return Generator: 生成器
        """
        # 捕获异常
        try:
            # 获取 tbody_html
            tbody_html = re.findall(r'<tbody>(.*?)</tbody>', html, re.S)[0]
            # 获取 tr_list
            tr_list = re.findall(r'<tr>(.*?)</tr>', tbody_html, re.S)
            # 遍历
            for tr_html in tr_list:
                # 获取 ip 字段
                ip = re.findall(r'<td.*?>(.*?)</td>', tr_html)[0]
                # 获取 port 字段
                port = re.findall(r'<td.*?>(.*?)</td>', tr_html)[1]
                # 获取 anonymity 字段
                anonymity = re.findall(r'<td.*?>(.*?)</td>', tr_html)[2]
                # 获取 ip_type 字段
                ip_type = re.findall(r'<td.*?>(.*?)</td>', tr_html)[3].lower()
                # 获取 ip_address 地址
                ip_address = re.findall(r'<td.*?>(.*?)</td>', tr_html)[4]
                # 获取 ip_response
                ip_response = re.findall(r'<td.*?>(.*?)</td>', tr_html)[5]
                # 获取 ip_time
                ip_time = re.findall(r'<td.*?>(.*?)</td>', tr_html)[6]
                # 获取 proxy_dict
                proxy_dict = {
                    "ip": ip,
                    "port": port,
                    "anonymity": anonymity,
                    "ip_type": ip_type,
                    "ip_address": ip_address,
                    "ip_response": ip_response,
                    "ip_time": ip_time
                }
                # generator
                yield proxy_dict
        except Exception as e:
            # 输出 log 信息
            print(f"Method get_proxy: the error of getting field: {e}")

    def all_proxies(self, header_list: list) -> list:
        """
        Takes all proxies by parsing html of web pages.

        :param header_list: 请求头列表
        :return all_proxies_list: 所有的 ip 代理池
        """
        # 输出 log 信息
        print("Method all_proxies: loading the module of all_proxies ....")
        # 声明 all_proxies_list，用于接收元素
        all_proxies_list = list()
        # for 循环
        for page in range(1, 31):
            # 拼接 url
            url = self.initial_url + "inha/" + str(page) + "/"
            # 输出 log 日志
            print("请求的 url：", url)
            # 获取网页源码
            html = self.get_page(url=url, header_list=header_list)
            # 条件判断
            if html is None:
                # 跳出循环
                break
            else:
                # 遍历
                for proxy_dict in self.get_proxy(html=html):
                    # 列表添加元素
                    all_proxies_list.append(proxy_dict)
        # 条件判断
        if all_proxies_list:
            # 占位符
            pass
        else:
            # 赋值
            all_proxies_list = self.read_txt()[0:30]
        # 输出 log 信息
        print("Method all_proxies: exiting the module of all_proxies ....")
        # 返回 all_proxies_list
        return all_proxies_list

    @staticmethod
    def verify_proxies(all_proxies_list: list) -> list:
        """
        Verify effectiveness of proxies.

        :param all_proxies_list: 所有代理 ip 列表
        :return verified_proxies_list: 有效的 ip 代理池
        """
        # 输出 log 信息
        print("Method verify_proxies: loading the module of verify_proxies ....")
        # 声明 verified_proxies_list，用于接收元素
        verified_proxies_list = list()
        # 遍历
        for proxies_dict in all_proxies_list:
            # 获得 ip_type
            ip_type = proxies_dict["ip_type"]
            # 获得 ip
            ip = proxies_dict["ip"]
            # 获得 port
            port = proxies_dict["port"]
            # 获取 back_info
            back_info = os.system("ping -c 1 -W 10 %s" % ip)
            # 条件判断
            if back_info:
                # 输出 log 信息
                print(ip)
                # 输出 log 信息
                print("ip error!")
            else:
                # 输出 log 信息
                print(ip)
                # 输出 log 信息
                print("ping successfully!")
                # 设置代理 ip
                proxies = {
                    ip_type: str(ip) + ":" + str(port)
                }
                # 列表添加元素
                verified_proxies_list.append(proxies)
        # 输出 log 信息
        print("Method verify_proxies: exiting the module of verify_proxies ....")
        # 返回 verified_proxies_list
        return verified_proxies_list

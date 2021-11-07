#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Author: StudentCWZ
# @Date: 2021/10/30 4:00 下午
# @File: use_headers.py


# 基本库
import json
import os
import sys


class HeadersBase(object):
    """Base class that all setting headers implementations derive from"""

    def __init__(self):
        """Declare variable"""
        # 声明 dir_path
        self.dir_path = os.path.split(os.path.realpath(__file__))[0]
        # 声明 file_path
        self.file_path = os.path.join(self.dir_path, "fake_useragent.json")

    def file_exists(self) -> bool:
        """
        Judge the existence of file.

        :return True/False
        """
        # 返回 True/False
        return os.path.exists(self.file_path)

    def get_headers(self) -> list:
        """
        Takes list of headers.

        :return header_list：请求头列表
        """
        # 条件判断
        if not self.file_exists():
            # 输出 debug 信息
            print("Method get_headers: the file of fake_useragent.json is not exist ...")
            # 输出 debug 信息
            print("Method get_headers: exiting the program ...")
            # 退出程序
            sys.exit(1)
        # 打开文件
        with open(self.file_path, "r", encoding="utf-8") as f:
            # 获取 header_list
            header_list = json.loads(f.read()).get("browsers", {}).get("chrome", [""])
            # 文件关闭
            f.close()
        # 条件判断
        if not header_list:
            # 输出 debug 信息
            print("Method get_headers: the error of getting header_list from fake_useragent.json ...")
            # 输出 debug 信息
            print("Method get_headers: exiting the program ...")
            # 退出程序
            sys.exit(1)
        # 返回 header_list
        return header_list

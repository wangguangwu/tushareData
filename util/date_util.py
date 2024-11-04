#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu
from datetime import datetime


def get_today_yyyymmdd():
    """
    返回今天的日期，格式为 yyyymmdd
    """
    return datetime.today().strftime('%Y%m%d')


def get_fixed_time():
    """
    返回固定时间
    """
    return "20230101"

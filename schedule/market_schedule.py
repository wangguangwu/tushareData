#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu
# 定时任务设置，每天上午10点执行一次
from apscheduler.schedulers.blocking import BlockingScheduler

from market import daily_basic
from market import daily
from basic import stock_basic
from util import date_util


def fetch_and_save_reference():
    """
    获取所有符合条件的 ts_code，并对每个 ts_code 调用 fetch_and_save_stock_data
    """
    # 调用 stock_basic 中的 fetch_on_stock_ts_code 方法获取 ts_code 列表
    ts_code_list = stock_basic.fetch_on_stock_ts_code()

    # 检查是否有符合条件的 ts_code
    if not ts_code_list:
        print("没有符合条件的股票代码。")
        return

    for ts_code in ts_code_list:
        try:
            print(f"Processing {ts_code}...")
            daily_basic.fetch_and_save_daily_basic(ts_code, date_util.get_fixed_time(), "")
            daily.fetch_and_save_stock_data(ts_code, date_util.get_fixed_time(), "")
        except Exception as e:
            print(f"处理 {ts_code} 时出错: {e}")

    print("market 定时任务执行结束")


scheduler = BlockingScheduler()
scheduler.add_job(fetch_and_save_reference, 'cron', 11, 11)

# 开始定时任务
scheduler.start()

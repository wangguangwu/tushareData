#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu
from apscheduler.schedulers.blocking import BlockingScheduler

from basic import stock_basic

from reference import top10_holders
from reference import top10_floatholders
from util import date_util


def fetch_and_save_all_stocks():
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
            top10_holders.fetch_and_save_top10_holders(ts_code, date_util.get_fixed_time(), "")
            top10_floatholders.fetch_and_save_top10_floatholders(ts_code, date_util.get_fixed_time(), "")
        except Exception as e:
            print(f"处理 {ts_code} 时出错: {e}")

    print("执行结束")


scheduler = BlockingScheduler()
scheduler.add_job(fetch_and_save_all_stocks, 'cron', 15, 37)
# 开始定时任务
scheduler.start()

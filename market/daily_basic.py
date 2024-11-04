#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu

import numpy as np
import pymysql

from config.config import pro, db_config


def fetch_daily_basic(ts_code, start_date, end_date):
    """
    从 Tushare 获取股票数据并返回 DataFrame
    """
    df = pro.daily_basic(**{
        "ts_code": ts_code,
        "trade_date": "",
        "start_date": start_date,
        "end_date": end_date,
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
        "limit_status"
    ])
    print("数据获取成功")
    print(df)
    return df

def save_daily_basic_to_db(df):
    """
    将股票每日指标数据 DataFrame 写入 MySQL 数据库，插入前检查是否已存在
    """
    # 将 NaN 值替换为 None，以便 MySQL 可以接受 NULL 值
    # 替换无效值，以确保数据能正确插入
    df = df.replace([np.inf, -np.inf], np.nan)  # 替换无穷大为 NaN
    df = df.fillna(0)  # 将 NaN 替换为 0

    # 确认替换成功
    if df.isnull().values.any():
        print("DataFrame 仍包含 NaN 值，替换未成功")
        return  # 停止执行，避免插入报错

    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            # 获取所有股票代码和交易日期的列表
            ts_code_list = df['ts_code'].unique().tolist()
            trade_dates = df['trade_date'].unique().tolist()

            # 如果列表为空，直接返回，不执行查询
            if not ts_code_list or not trade_dates:
                print("ts_code_list 或 trade_dates 列表为空，跳过查询")
                return

            # 构建查询语句，使用多个占位符来替代 IN 条件
            query_sql = f"""
                SELECT ts_code, trade_date FROM daily_basic
                WHERE ts_code IN ({", ".join(["%s"] * len(ts_code_list))}) 
                AND trade_date IN ({", ".join(["%s"] * len(trade_dates))})
            """
            # 执行查询，将 ts_code_list 和 trade_dates 展开作为参数
            cursor.execute(query_sql, ts_code_list + trade_dates)
            existing_records = set((row[0], row[1]) for row in cursor.fetchall())

            # 准备要插入的数据
            data_to_insert = []
            for index, row in df.iterrows():
                record_key = (row['ts_code'], row['trade_date'])
                if record_key not in existing_records:  # 仅插入数据库中不存在的记录
                    data_to_insert.append((
                        row['ts_code'], row['trade_date'], row['close'], row['turnover_rate'],
                        row['turnover_rate_f'], row['volume_ratio'], row['pe'], row['pe_ttm'],
                        row['pb'], row['ps'], row['ps_ttm'], row['dv_ratio'], row['dv_ttm'],
                        row['total_share'], row['float_share'], row['free_share'], row['total_mv'],
                        row['circ_mv'], row['limit_status']
                    ))

            # 批量插入数据
            if data_to_insert:
                insert_sql = """
                    INSERT INTO daily_basic (
                        ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio,
                        pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share,
                        free_share, total_mv, circ_mv, limit_status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, data_to_insert)
                connection.commit()
                print(f"{len(data_to_insert)} 条新数据已插入")
            else:
                print("没有需要插入的新数据")
    finally:
        connection.close()

def fetch_and_save_daily_basic(ts_code, start_date, end_date):
    """
    根据 ts_code 获取并保存股票数据
    """
    # 调用 fetch_stock_data 获取数据
    df = fetch_daily_basic(ts_code, start_date, end_date)

    # 调用 save_stock_data_to_db 保存数据
    save_daily_basic_to_db(df)

    print(f"{ts_code} 的数据已成功获取并保存")

# 使用示例
if __name__ == "__main__":
    fetch_and_save_daily_basic("000415.SZ", "", "")
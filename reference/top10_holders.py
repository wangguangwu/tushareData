#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu

# 导入tushare
import numpy as np
import pymysql

from config.config import pro, db_config

def fetch_top10_holders(ts_code, start_date, end_date):
    """
    从 Tushare 获取前十大股东数据并返回 DataFrame
    """
    df = pro.top10_holders(**{
        "ts_code": ts_code,
        "period": "",
        "ann_date": "",
        "start_date": start_date,
        "end_date": end_date,
        "offset": "",
        "limit": "",
        "hold_type": ""
    }, fields=[
        "ts_code",
        "ann_date",
        "end_date",
        "holder_name",
        "hold_amount",
        "hold_ratio",
        "hold_float_ratio",
        "hold_change",
        "holder_type"
    ])
    print("数据获取成功")
    print(df)
    return df

def save_top10_holders_to_db(df):
    """
    将前十大股东数据 DataFrame 写入 MySQL 数据库，插入前检查是否已存在
    """
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
            # 获取所有股票代码和公告日期的列表
            ts_code_list = df['ts_code'].unique().tolist()
            ann_dates = df['ann_date'].unique().tolist()

            # 如果列表为空，直接返回，不执行查询
            if not ts_code_list or not ann_dates:
                print("ts_code_list 或 ann_dates 列表为空，跳过查询")
                return

            # 构建查询语句，使用多个占位符来替代 IN 条件
            query_sql = f"""
                SELECT ts_code, ann_date FROM top10_holders
                WHERE ts_code IN ({", ".join(["%s"] * len(ts_code_list))}) 
                AND ann_date IN ({", ".join(["%s"] * len(ann_dates))})
            """
            # 执行查询，将 ts_code_list 和 ann_dates 展开作为参数
            cursor.execute(query_sql, ts_code_list + ann_dates)
            existing_records = set((row[0], row[1]) for row in cursor.fetchall())

            # 准备要插入的数据
            data_to_insert = []
            for index, row in df.iterrows():
                record_key = (row['ts_code'], row['ann_date'])
                if record_key not in existing_records:  # 仅插入数据库中不存在的记录
                    data_to_insert.append((
                        row['ts_code'], row['ann_date'], row['end_date'], row['holder_name'],
                        row['hold_amount'], row['hold_ratio'], row['hold_float_ratio'],
                        row['hold_change'], row['holder_type']
                    ))

            # 批量插入数据
            if data_to_insert:
                insert_sql = """
                    INSERT INTO top10_holders (
                        ts_code, ann_date, end_date, holder_name, hold_amount,
                        hold_ratio, hold_float_ratio, hold_change, holder_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, data_to_insert)
                connection.commit()
                print(f"{len(data_to_insert)} 条新数据已插入")
            else:
                print("没有需要插入的新数据")
    finally:
        connection.close()

def fetch_and_save_top10_holders(ts_code, start_date, end_date):
    """
    根据 ts_code 获取并保存前十大股东数据
    """
    # 调用 fetch_top10_holders 获取前十大股东数据
    df = fetch_top10_holders(ts_code, start_date, end_date)

    # 调用 save_top10_holders_to_db 保存数据
    save_top10_holders_to_db(df)

    print(f"{ts_code} 的前十大股东数据已成功获取并保存")

# 使用示例
if __name__ == "__main__":
    fetch_and_save_top10_holders("600221.SH", "20230101", "")  # 调用方法，传入 ts_code


#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu
import pymysql

from config.config import pro, db_config


def fetch_stock_data(ts_code, start_date, end_date):
    """
    从 Tushare 获取股票数据并返回 DataFrame
    """
    df = pro.daily(
        ts_code=ts_code,
        trade_date="",
        start_date=start_date,
        end_date=end_date,
        offset="",
        limit=""
    )
    print("数据获取成功")
    return df


def save_stock_data_to_db(df):
    """
    将股票数据 DataFrame 写入 MySQL 数据库，插入前检查是否已存在
    """
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
                SELECT ts_code, trade_date FROM stock_daily_data
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
                        row['ts_code'], row['trade_date'], row['open'], row['high'], row['low'],
                        row['close'], row['pre_close'], row['change'], row['pct_chg'], row['vol'], row['amount']
                    ))

            # 批量插入数据
            if data_to_insert:
                insert_sql = """
                    INSERT INTO stock_daily_data (ts_code, trade_date, open, high, low, close, pre_close, `change`, pct_chg, vol, amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, data_to_insert)
                connection.commit()
                print(f"{len(data_to_insert)} 条新数据已插入")
            else:
                print("没有需要插入的新数据")
    finally:
        connection.close()


def fetch_and_save_stock_data(ts_code, start_date, end_date):
    """
    根据 ts_code 获取并保存股票数据
    """
    # 调用 fetch_stock_data 获取数据
    df = fetch_stock_data(ts_code, start_date, end_date)

    # 调用 save_stock_data_to_db 保存数据
    save_stock_data_to_db(df)

    print(f"{ts_code} 的数据已成功获取并保存")


# 使用示例
if __name__ == "__main__":
    fetch_and_save_stock_data("600221.SH", "", "")  # 调用方法，传入 ts_code

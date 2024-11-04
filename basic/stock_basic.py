#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu


# 导入tushare
import pymysql
from config.config import pro, db_config

def fetch_stock_basic_data():
    """
    从 Tushare 获取股票基本信息数据并返回 DataFrame
    """
    df = pro.stock_basic(
        ts_code="", name="", exchange="", market="", is_hs="", list_status="", limit="", offset="",
        fields=[
            "ts_code", "symbol", "name", "area", "industry", "cnspell", "market",
            "list_date", "act_name", "act_ent_type", "fullname", "enname",
            "exchange", "curr_type", "list_status", "delist_date", "is_hs"
        ]
    )
    print("数据获取成功")
    return df


def save_stock_basic_data_to_db(df):
    """
    将股票基本信息数据 DataFrame 写入 MySQL 数据库，插入前检查是否已存在
    """
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            # 查询数据库中已有的记录，避免重复插入
            ts_codes = df['ts_code'].tolist()
            query_sql = """
                SELECT ts_code FROM stock_basic_info WHERE ts_code IN (%s)
            """ % ", ".join(["%s"] * len(ts_codes))
            cursor.execute(query_sql, ts_codes)
            existing_codes = set(row[0] for row in cursor.fetchall())

            # 准备需要插入的数据
            data_to_insert = []
            for index, row in df.iterrows():
                if row['ts_code'] not in existing_codes:
                    data_to_insert.append((
                        row['ts_code'], row['symbol'], row['name'], row['area'], row['industry'],
                        row['cnspell'], row['market'], row['list_date'], row['act_name'],
                        row['act_ent_type'], row['fullname'], row['enname'], row['exchange'],
                        row['curr_type'], row['list_status'], row['delist_date'], row['is_hs']
                    ))

            # 批量插入数据
            if data_to_insert:
                insert_sql = """
                    INSERT INTO stock_basic_info (ts_code, symbol, name, area, industry, cnspell, market, list_date,
                                                  act_name, act_ent_type, fullname, enname, exchange, curr_type,
                                                  list_status, delist_date, is_hs)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, data_to_insert)
                connection.commit()
                print(f"{len(data_to_insert)} 条新数据已插入")
            else:
                print("没有需要插入的新数据")
    finally:
        connection.close()


def fetch_and_save_stock_basic_data():
    """
    拉取并保存股票基本信息数据
    """
    # 获取数据
    df = fetch_stock_basic_data()

    # 保存数据
    save_stock_basic_data_to_db(df)

    print("股票基本信息数据已成功获取并保存")


def fetch_on_stock_ts_code():
    """
    查询数据库，返回 list_status 为 'L' 的 ts_code
    """
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            # 查询 list_status 为 'L' 的 ts_code
            query_sql = "SELECT ts_code FROM stock_basic_info WHERE list_status = 'L'"
            cursor.execute(query_sql)
            results = cursor.fetchall()  # 获取所有查询结果

            # 返回结果作为列表（提取第一个元素 ts_code）
            return [row[0] for row in results]
    finally:
        connection.close()


# 使用示例
# if __name__ == "__main__":
#     fetch_and_save_stock_basic_data()  # 调用汇总方法，拉取并保存数据

if __name__ == "__main__":
    active_stocks = fetch_on_stock_ts_code()
    for ts_code in active_stocks:
        print(f"TS Code: {ts_code}")

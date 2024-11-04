#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

# author: wangguangwu
import yaml
import tushare as ts

# # 读取配置文件
# with open('config.yaml', 'r') as file:
#     config = yaml.safe_load(file)
#
# # 获取 API 密钥
# api_key = config['API']['api_key']
# pro = ts.pro_api(api_key)
#
# # 获取数据库配置
# db_config = {
#     'host': config['DATABASE']['host'],
#     'user': config['DATABASE']['user'],
#     'password': config['DATABASE']['password'],
#     'database': config['DATABASE']['database'],
#     'charset': config['DATABASE']['charset']
# }

import os
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'config.yaml')

with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

# 获取 API 密钥
api_key = config['API']['api_key']
pro = ts.pro_api(api_key)

# 获取数据库配置
db_config = {
    'host': config['DATABASE']['host'],
    'user': config['DATABASE']['user'],
    'password': config['DATABASE']['password'],
    'database': config['DATABASE']['database'],
    'charset': config['DATABASE']['charset']
}
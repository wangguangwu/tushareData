# tushareData

访问 [tushare](https://tushare.pro/) 获取数据


```shell
├── basic
│   └── stock_basic.py               # 股票基础信息模块
├── config
│   ├── config.py                    # 配置文件，包含数据库、API 等配置
│   └── config.yaml                  # YAML 格式的配置文件
├── financial                        # 财务相关模块（空）
├── market
│   ├── daily.py                     # 市场行情每日数据
│   └── daily_basic.py               # 基础市场数据
├── reference
│   ├── top10_floatholders.py        # 前十大流通股东数据模块
│   └── top10_holders.py             # 前十大股东数据模块
├── schedule
│   ├── market_schedule.py           # 市场调度任务
│   └── reference_schedule.py        # 参考数据调度任务
└── util
    └── date_util.py                 # 日期工具模块
```
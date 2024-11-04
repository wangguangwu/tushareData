
# 导入tushare
import tushare as ts
# 初始化pro接口
pro = ts.pro_api('7c0b962c215058d6fcf4085f586a0e789827e9b131326f0e4deef9a2')

# 拉取数据
df = pro.top10_floatholders(**{
    "ts_code": "",
    "period": "",
    "ann_date": "",
    "start_date": "",
    "end_date": "",
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
print(df)


import dolphindb as ddb

import numpy as np
import pandas as pd

import os
import logging
import time
import re

logging.getLogger().setLevel(logging.INFO)

HOST = '172.24.148.36'
PORT = 8992
USERID = 'gyreader'
PASSWORD = '123456'

data_dir = "E:/python_projects/ddb/AllData/FutureData/dayKline_full_period"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

session = ddb.session()
session.connect(host=HOST, port=PORT, userid=USERID, password=PASSWORD)

commodity_fut_code = pd.read_csv("future_basic_info.csv")[["fut_code", "exchange"]].\
    drop_duplicates(subset=["fut_code", "exchange"], keep="last").reset_index(drop=True)
financial_fut_code = pd.DataFrame(
    {
        "fut_code": ["IF", "IC", "IM", "IH", "TL", "T", "TF", "TS"],
        "exchange": ["CFFEX"] * 8
    }
)
fut_code = pd.concat([commodity_fut_code, financial_fut_code], axis=0)

tushare_code_to_wind_code_map = {
    "DCE": "DCE",      # 大商所
    "SHFE": "SHF",     # 上海期货交易所
    "CZCE": "CZC",     # 郑商所
    "CFFEX": "CFE",    # 中金所
    "INE": "INE",      # 上期能源
    "GFEX": "GFE"      # 广期所
}


# 不同期货品种对应WIND数据库的表不同
# 商品期货：WIND.CCommodityFuturesEODPrices
# 股指期货：WIND.CIndexFuturesEODPrices
# 国债期货：WIND.CBondFuturesEODPrices
fut_code_table_map = {
    "IF": "WIND.CINDEXFUTURESEODPRICES",
    "IC": "WIND.CINDEXFUTURESEODPRICES",
    "IM": "WIND.CINDEXFUTURESEODPRICES",
    "IH": "WIND.CINDEXFUTURESEODPRICES",
    "TL": "WIND.CBONDFUTURESEODPRICES",
    "T": "WIND.CBONDFUTURESEODPRICES",
    "TF": "WIND.CBONDFUTURESEODPRICES",
    "TS": "WIND.CBONDFUTURESEODPRICES"
}


start_date = "1997.06.30"
end_date = "2026.03.27"


for _, (code, exchange) in fut_code.iterrows():

    if code not in fut_code_table_map:
        table_name = "WIND.CCOMMODITYFUTURESEODPRICES"
    else:
        table_name = fut_code_table_map[code]

    if not re.match("^[a-zA-Z]+$", code):
        logging.info(f"{code}品种不是generic future contract,跳过")
        continue

    _query_time_start = time.time()
    _sql_scripts = f"""
    res = 
    SELECT
        S_INFO_WINDCODE as contract_code,
        TRADE_DT as trade_date,
        S_DQ_PRESETTLE as pre_settle_price,
        S_DQ_OPEN as open_price,
        S_DQ_HIGH as high_price,
        S_DQ_LOW as low_price,
        S_DQ_CLOSE as close_price,
        S_DQ_SETTLE as settle_price,
        S_DQ_VOLUME as volume,
        S_DQ_AMOUNT as amount,
        S_DQ_OI as interest
    FROM loadTable("dfs://{table_name}", "data")
    WHERE regexFind(S_INFO_WINDCODE, "{code}[0-9]{{4}}\.{tushare_code_to_wind_code_map[exchange]}")>=0
    AND TRADE_DT>={start_date} AND TRADE_DT<={end_date}
    ORDER BY TRADE_DT asc
    res;
    """

    df = session.run(_sql_scripts)
    if not df.empty:
        parquet_path = data_dir + f"/{code}.parquet"
        df.to_parquet(parquet_path)

        logging.info(f"{code}品种日线数据已保存到{parquet_path}")
        logging.info(f"{code}品种日线数据查询耗时：{time.time()-_query_time_start}")
        logging.info(f"{code}品种日线数据共{len(df)}条")
        logging.info(f"parquet文件大小: {os.path.getsize(parquet_path)/1024/1024} MB")
    
    else:
        logging.info(f"{exchange}:{code}品种日线数据为空")
    
    time.sleep(1)





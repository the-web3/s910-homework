import requests
from decimal import Decimal
from datetime import datetime, timezone
import pymysql
#向这个地址请求数据，把服务器的响应保存到变量 response
response=requests.get(
     "https://api.coinbase.com/v2/prices/BTC-USD/spot",
     timeout=15
)
#检查 HTTP 状态；如果服务器返回错误，就报错
response.raise_for_status()

#print(response.json())
#取出外层 data 对应的内容
data=response.json()["data"]

#从里面取出价格，保存到变量 price
price=Decimal(data["amount"])
sampled_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

row = ("coinbase", "BTC", sampled_at, price)

print(row)
#把价格显示出来
print("btc价格",price,"USD")

with pymysql.connect(
    host="127.0.0.1",
    port=9030,
    user="root",
    password="",
    database="market",
    autocommit=True
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO price_snapshots
                (source, symbol, sampled_at, price_usd)
            VALUES (%s, %s, %s, %s)
            """,
            row
        )

print("写入成功")
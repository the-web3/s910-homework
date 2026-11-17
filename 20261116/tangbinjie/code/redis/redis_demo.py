import redis
client = redis.Redis(host="127.0.0.1",port=6379,decode_responses=True)
print(client.ping())
#print(client.set("product:1001:name", "机械键盘"))
#print(client.expire("product:1001:name", 30))
print(client.get("product:1001:name"))
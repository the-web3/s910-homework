import json
import uuid

from confluent_kafka import Producer


# 1. 创建生产者
producer = Producer({
    "bootstrap.servers": "localhost:9092",

    # 配合本地 Docker 的 IPv4 端口映射
    "broker.address.family": "v4",

    # 开启生产者幂等，减少内部重试造成的重复写入
    "enable.idempotence": True,

    # 等待同步副本确认
    "acks": "all",

    # 消息投递最多等待 10 秒
    "message.timeout.ms": 10000,
})

delivery_errors = []


# 2. Kafka 返回投递结果后，执行这个回调
def on_delivery(error, message):
    if error is not None:
        delivery_errors.append(str(error))
        print(f"发送失败：{error}")
    else:
        print(
            f"发送成功："
            f"key={message.key().decode('utf-8')}，"
            f"partition={message.partition()}，"
            f"offset={message.offset()}"
        )


# 每次运行使用不同的订单编号前缀
run_id = uuid.uuid4().hex[:8]

# 3. 构造并发送消息
try:
    for number in range(1, 6):
        order = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order.created",
            "schema_version": 1,
            "order_id": f"ORDER-{run_id}-{number}",
            "amount_cents": number * 1000,
        }

        producer.produce(
            topic="orders-demo",

            # 使用订单编号作为消息 key
            key=order["order_id"].encode("utf-8"),

            # 字典 → JSON 字符串 → 字节
            value=json.dumps(
                order,
                ensure_ascii=False,
            ).encode("utf-8"),

            on_delivery=on_delivery,
        )

        # 执行已经到达的投递回调
        producer.poll(0)

finally:
    # 4. 退出前等待发送完成，同时执行剩余回调
    pending = producer.flush(15)

# 5. 检查是否全部发送成功
if pending > 0 or delivery_errors:
    raise RuntimeError(
        f"发送未全部成功：未完成={pending}，"
        f"投递错误={delivery_errors}"
    )

print("5 条订单消息全部发送成功")
import json

from confluent_kafka import Consumer, KafkaException


# 1. 创建消费者
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "broker.address.family": "v4",

    # 消费者组名称
    "group.id": "order-notify",

    # 没有有效的已提交进度时，从最早可用消息开始
    "auto.offset.reset": "earliest",

    # 业务处理完成后，由代码手动提交进度
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
})


# 2. 定义业务处理函数
def handle_order(order):
    # 练习中只打印；实际项目可在这里写数据库、发送通知
    print(
        f"模拟处理订单："
        f"订单编号={order['order_id']}，"
        f"金额分={order['amount_cents']}，"
        f"事件编号={order['event_id']}"
    )


processed = 0

try:
    # 3. 订阅 Topic
    consumer.subscribe(["orders-demo"])

    # 成功处理 5 条消息后退出
    while processed < 5:

        # 最多等待 1 秒，获取一条消息
        message = consumer.poll(1.0)

        # 暂时没有消息，继续等待
        if message is None:
            continue

        # Kafka 返回错误时，终止运行
        if message.error():
            raise KafkaException(message.error())

        if message.value() is None:
            raise ValueError("订单消息内容不能为空")

        # 4. 字节 → JSON 字符串 → 字典
        order = json.loads(
            message.value().decode("utf-8")
        )

        # 5. 先处理业务
        handle_order(order)

        # 6. 业务成功后，再同步提交进度
        committed = consumer.commit(
            message=message,
            asynchronous=False,
        )

        # 检查分区提交结果
        for partition in committed:
            if partition.error is not None:
                raise KafkaException(partition.error)

        processed += 1

        print(
            f"消费完成："
            f"partition={message.partition()}，"
            f"offset={message.offset()}"
        )

except KeyboardInterrupt:
    print("用户停止消费")

finally:
    # 7. 释放连接并退出消费者组
    consumer.close()
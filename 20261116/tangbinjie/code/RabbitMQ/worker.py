import pika

credentials = pika.PlainCredentials("learner", "learn123")

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="127.0.0.1", credentials=credentials)
)
channel = connection.channel()

# 队列名称和配置必须与发送端一致
channel.queue_declare(queue="task_queue", durable=True)


# 每收到一条消息，Pika 就会调用这个函数
def handle_message(ch, method, properties, body):
    print("收到任务：", body.decode("utf-8"))

    # 这里用打印模拟任务处理
    print("模拟处理完成")

    # 告诉 RabbitMQ：这条消息已处理完成
    ch.basic_ack(delivery_tag=method.delivery_tag)


# 指定从哪个队列接收，以及收到消息后调用哪个函数
channel.basic_consume(
    queue="task_queue",
    on_message_callback=handle_message,
    auto_ack=False
)

print("等待任务中，按 Ctrl+C 退出")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
finally:
    connection.close()
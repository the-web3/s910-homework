import pika
credentials = pika.PlainCredentials("learner", "learn123")
connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1", credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue="task_queue", durable=True)
channel.basic_publish(exchange="", routing_key="task_queue", body="生成报表")
connection.close()
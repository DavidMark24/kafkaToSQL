from confluent_kafka import Consumer


class KafkaConsumer:

    def __init__(self, bootstrap_server, group_id):
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        self.consumer = Consumer({
            'bootstrap.server': self.bootstrap_server,
            'group_id': self.group_id,
            'auto.offset': 'earliest'
        })

    def subscribe(self, topics):
        self.consumer.subscribe(topics)

    def consume_message(self):
        while True:
            message = self.consumer.poll(1.0)
            if message is None:
                continue

            if message.error():
                print(f"Error: {message.error()}")
                continue
            value = message.value().decode("utf-8")
            yield value




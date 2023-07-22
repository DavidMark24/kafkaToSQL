from producer import KafkaProducer
from consumer import KafkaConsumer
from utils.database import Database
import logging

logging.basicConfig(filename='data/main.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


class Main:

    def __init__(self):
        self.bootstrap = "localhost:9092"
        self.topic = "employee"

    def read_file(self, file):
        try:
            producer = KafkaProducer(self.bootstrap)
            with open(file, 'r') as emp:
                for line in emp:
                    producer.produce_mess(self.topic, line.rstrip())
            producer.flush()
        except Exception as e:
            logging.error(f'An error occurred while reading the config file: {str(e)}')

    def consume(self):
        try:
            conn = Database("data/config.ini")
            conn.connect()
            consumer = KafkaConsumer(self.bootstrap, 'employee-consumer-group')
            consumer.subscribe([self.topic])
            for message in consumer.consume_message():
                conn.insert_data("employee", message)
            conn.cursor.close()
        except Exception as e:
            logging.error(f'An error occurred while subscribing topic to mySQL: {str(e)}')


if __name__ == '__main__':
    start_data = Main()
    start_data.read_file("data/employee.csv")
    start_data.consume()

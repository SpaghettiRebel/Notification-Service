from confluent_kafka import Producer


class KafkaEventPublisher:
    def __init__(self, producer):
        producer = Producer()

    @staticmethod
    def delivery_callback(err, msg):
        topic = msg.topic()
        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')

        if err:
            print(f'ERROR: Message failed delivery: {err}')
        else:
            print(f'Produced event to topic {topic}: key = {key:12} value = {value:12}')

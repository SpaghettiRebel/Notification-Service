from confluent_kafka import Producer


class KafkaEventPublisher:
    def __init__(self, producer):
        producer = Producer()

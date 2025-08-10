import logging
from confluent_kafka import KafkaError, KafkaException

class BaseConsumer():
    def __init__(self, group_id):
        self.BOOTSTRAP_SERVERS = 'kafka01:9092,kafka02:9092,kafka03:9092'
        self.group_id = group_id
        self.logger = self.get_logger(group_id)     #   <- 인스턴스변수 logger

    def get_logger(self, group_id):
        logger = logging.getLogger(group_id)  # 로거 생성  <- 지역변수 logger
        logger.setLevel(logging.INFO)        # 작업수준 설정
        handler = logging.StreamHandler()   # 화면에 기록하도록 설정
        handler.setFormatter(              #기록할 형식
            logging.Formatter('%(asctime)s [%(levelname)s]:%(message)s'))
        logger.addHandler(handler)  # 핸들러 적용
        return logger

    def callback_on_assign(self, consumer, partition):
        self.logger.info(f'consumer:{consumer}. assigned partition: {partition}')

    def handle_error(self, msg, error):
        if error.code() == KafkaError._PARTITION_EOF:  # 다 읽어서 발생하는 에러:debug(낮은단계)
            self.logger.debug(f'End of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        else:
            # 기타 에러 발생시 종료처리
            self.logger.error(f"Kafka error occurred: {error.str()} "
                              f"on topic {msg.topic()} partition {msg.partition()} "
                              f"at offset {msg.offset()}")
            raise KafkaException(error)
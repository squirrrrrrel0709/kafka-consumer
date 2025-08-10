from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import sys
import json
import time

class ConsumeConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics = ['apis.seouldata.rt-bicycle']

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',  # 최근거부터 마저읽기
                'enable.auto.commit': 'false' # 오토커밋 끄기
                }

        self.consumer = Consumer(conf)     # 파티션 할당받으면 콜백(할당완료 로그)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:  # 메세지 100개 읽어서 리스트 저장
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue  # 메세지 없으면 다음루프

                self.logger.info(f'message count:{len(msg_lst)}')  # 메세지 길이 로그찍기
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error)  # 베이스컨슈머 메서드


                self.logger.info(f'message 처리 로직 시작')
                # 메세지 값을 디코딩하고, JSON문자열을 파이썬 객체로 반환
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                # DF로 만들어서 찍어줌
                df = pd.DataFrame(msg_val_lst)
                print(df[:10])


                self.logger.info(f'message 처리 로직 완료, Async Commit 후 2초 대기')
                # 오토커밋 꺼져있으므로: 로직 처리 완료 후 Async Commit 수행
                self.consumer.commit(asynchronous=True)
                self.logger.info(f'Commit 완료')
                time.sleep(2)

        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    consume_consumer= ConsumeConsumer('consume_consumer')
    consume_consumer.poll()







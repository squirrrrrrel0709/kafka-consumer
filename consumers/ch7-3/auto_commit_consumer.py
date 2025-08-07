from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import json


class AutoCommitConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id) #상속:서버,로거,group_id 받아옴 -> consumer 생성
        self.topics = ['apis.seouldata.rt-bicycle'] # 구독할 토픽이름 -> 구독할때 필요

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS, #컨슈머 설정
                'group.id': self.group_id,
                'auto.offset.reset': 'latest', # 최근거부터 마저읽기!
                'enable.auto.commit': 'true', # earliest에서 true로 바뀜
                'auto.commit.interval.ms': '60000'   # 기본 값: 5000 (5초) -> 60초
                }

        self.consumer = Consumer(conf) #라이브러리 제공 클래스로 컨슈머 생성
        # 해당토픽 구독, 성공시 callback_on_assign 실행
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:                       #메세지 100개 읽어서 리스트 저장
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue # 메세지 없으면 다음루프

                self.logger.info(f'message count:{len(msg_lst)}') #메세지길이 로그찍기
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error) # 베이스컨슈머 메서드

                # 로직 처리 부분
                self.logger.info(f'message 처리 로직 시작')
                # 메세지 값을 디코딩하고, JSON문자열을 파이썬 객체로 반환
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                # DF로 만들어서 찍어줌
                df = pd.DataFrame(msg_val_lst)
                print(df[:10])


        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt:  # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    auto_commit_consumer = AutoCommitConsumer('auto_commit_consumer')
    auto_commit_consumer.poll()
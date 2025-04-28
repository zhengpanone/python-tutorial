from kafka import KafkaConsumer
import json
import logging
from kafka.errors import KafkaError

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建 KafkaConsumer 实例
consumer = KafkaConsumer(
    "test",
    bootstrap_servers='127.0.0.1:9092',
    sasl_mechanism="PLAIN",
    security_protocol="SASL_PLAINTEXT",
    sasl_plain_username="jc",  # Kafka 配置中的用户名
    sasl_plain_password="jckafka",  # Kafka 配置中的密码
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True  # 自动提交偏移量
)

def consume_messages():
    try:
        for message in consumer:
            logger.info(f"Received message: {message.value} from topic: {message.topic} "
                        f"at partition: {message.partition} with offset: {message.offset}")
    except KafkaError as e:
        logger.error(f"Error while consuming messages: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped manually.")
    finally:
        # 优雅关闭消费者
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    consume_messages()

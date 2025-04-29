from kafka import KafkaProducer
import json
import logging
import time

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# 创建 KafkaProducer 实例（在应用生命周期内复用）
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    sasl_mechanism="PLAIN",
    security_protocol="SASL_PLAINTEXT",
    sasl_plain_username="jc",  # Kafka 配置中的用户名
    sasl_plain_password="jckafka",  # Kafka 配置中的密码
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # 确保消息被所有副本确认
    retries=3,  # 重试次数
    request_timeout_ms=120000,  # 设置2分钟超时
    batch_size=16384,  # 批量发送的大小
    linger_ms=10  # 消息发送的延迟时间
)


def send_message(producer, topic, message):
    """发送消息"""
    try:
        future = producer.send(topic, message)
        result = future.get(timeout=10)  # 等待消息发送完成
        logger.info(f"Message sent to topic '{topic}': {result}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        # 重试机制：若发送失败，可选择适当的重试逻辑
        # 简单的重试逻辑
        retries = 3
        while retries > 0:
            retries -= 1
            logger.info(f"Retrying... ({3 - retries} retries left)")
            time.sleep(5)  # 简单的延迟
            try:
                future = producer.send(topic, message)
                result = future.get(timeout=10)
                logger.info(
                    f"Message sent to topic '{topic}' after retry: {result}")
                break
            except Exception as e:
                if retries == 0:
                    logger.error(f"Failed to send message after retries: {e}")
                    break


def main():
    for index in range(10):
        # message = f"Hello Kafka {index}"
        message = {"key": f"value-{index}", "example": "message"}
        send_message(producer, 'test', message)


if __name__ == "__main__":
    main()

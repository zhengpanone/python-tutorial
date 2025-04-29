import json
import logging
import signal
import sys
from kafka import KafkaConsumer

from config import KAFKA_CONFIG, TOPICS

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('kafka-consumer')

# 合并配置
consumer_config = {**KAFKA_CONFIG, **KAFKA_CONFIG.get('consumer', {})}
# 移除嵌套的配置，避免冲突
if'producer'in consumer_config:
    del consumer_config['producer']
if'consumer'in consumer_config:
    del consumer_config['consumer']

# 创建Consumer实例
c = KafkaConsumer(**consumer_config)

# 标记是否正在关闭
shutting_down = False

def signal_handler(sig, frame):
    """处理终止信号，确保优雅关闭"""
    global shutting_down
    if shutting_down:
        return
    shutting_down = True
    logger.info("接收到终止信号，正在优雅关闭...")
    sys.exit(0)

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def process_message(msg):
    """处理接收到的消息

    Args:
        msg: Kafka消息对象
    """
    try:
        topic = msg.topic()
        value = msg.value().decode("utf-8")
        key = msg.key().decode("utf-8") if msg.key() else None

        # 尝试解析JSON
        try:
            payload = json.loads(value)
            logger.info(f'接收到消息 [主题: {topic}, 键: {key}]')
            logger.debug(f'消息内容: {payload}')
        except json.JSONDecodeError:
            logger.info(f'接收到非JSON消息 [主题: {topic}, 键: {key}]: {value}')

        # 根据主题类型处理不同的消息
        if topic == TOPICS['email']:
            handle_email_notification(payload if'payload'in locals() else value)
        elif topic == TOPICS['sms']:
            handle_sms_notification(payload if'payload'in locals() else value)
        else:
            logger.warning(f'收到未知主题的消息: {topic}')

    except Exception as e:
        logger.error(f'处理消息时出错: {e}')

def handle_email_notification(payload):
    """处理邮件通知"""
    # 这里实现实际的邮件发送逻辑
    logger.info(f'处理邮件通知: {payload}')

def handle_sms_notification(payload):
    """处理短信通知"""
    # 这里实现实际的短信发送逻辑
    logger.info(f'处理短信通知: {payload}')

def main():
    try:
        # 订阅主题
        topics_to_subscribe = list(TOPICS.values())
        logger.info(f'订阅主题: {topics_to_subscribe}')
        c.subscribe(topics_to_subscribe)

        logger.info('开始消费消息...')
        while not shutting_down:
            msg = c.poll(1.0)  # 超时时间1秒

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 到达分区末尾，不是错误
                    logger.debug(f'到达分区末尾: {msg.topic()} [{msg.partition()}]')
                    continue
                else:
                    # 其他错误
                    logger.error(f'Kafka错误: {msg.error()}')
                    break

            # 处理消息
            process_message(msg)

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        # 关闭消费者
        logger.info("关闭消费者...")
        c.close()
        logger.info("消费者已关闭")

if __name__ == "__main__":
    main()

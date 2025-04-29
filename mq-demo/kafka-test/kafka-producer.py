from kafka import KafkaProducer
import json
import logging
import config
import sys
import signal
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()]
                    )
logger = logging.getLogger(__name__)

# 合并配置
producer_config = {**config.KAFKA_CONFIG, **
                   config.KAFKA_CONFIG.get('producer', {})}

if 'producer' in producer_config:
    del producer_config['producer']

if 'consumer' in producer_config:
    del producer_config['consumer']

# 创建Producer实例
p = KafkaProducer(**producer_config)

# 标记是否正在关闭
shutting_down = False

def signal_handler(sig, frame):
    """处理终止信号，确保优雅关闭"""
    global shutting_down
    if shutting_down:
        return
    shutting_down = True
    logger.info("接收到终止信号，正在优雅关闭...")
    # 确保所有消息都被发送
    p.flush(10)  # 等待最多10秒
    logger.info("生产者已关闭")
    sys.exit(0)

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def delivery_report(err, msg):
    """消息发送后的回调函数"""
    if err is not None:
        logger.error(f"消息发送失败: {err}")
    else:
        logger.info(f"消息发送成功: {msg.topic} [{msg.partition}] @ {msg.offset}")
        
        
def send_notification(topic_key, payload, key=None):
    """发送通知消息到指定主题

    Args:
        topic_key: 主题键名（在TOPICS字典中定义）
        payload: 消息内容（字典或JSON字符串）
        key: 可选的消息键

    Returns:
        bool: 是否成功将消息加入发送队列
    """
    try:
        # 获取实际主题名
        topic = config.TOPICS.get(topic_key, topic_key)
         # 如果payload是字典，转换为JSON字符串
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        # 发送消息
        p.send(topic, value=payload, key=key.encode('utf-8') if key else None, callback=delivery_report)

        logger.info(f'消息已加入发送队列: {topic}')
        return True
    except Exception as e:
        logger.error(f'发送消息时出错: {e}')
        return False

if __name__ == "__main__":
    try:
        # 发送邮件通知
        email_payload = {
            "to": "receiver@example.com",
            "from": "sender@example.com",
            "subject": "Sample Email",
            "body": "This is a sample email notification"
        }
        send_notification('email', email_payload)

        # 发送短信通知
        sms_payload = {
            "phoneNumber": "1234567890",
            "message": "This is a sample SMS notification"
        }
        send_notification('sms', sms_payload)

        # 确保所有消息都被发送
        remaining = p.flush(timeout=5)
        if remaining > 0:
            logger.warning(f'仍有 {remaining} 条消息未发送完成')
        else:
            logger.info('所有消息已成功发送')

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        # 确保所有消息都被发送
        p.flush(timeout=5)
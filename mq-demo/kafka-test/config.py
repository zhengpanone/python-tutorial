import json
# Kafka配置文件

# Kafka服务器配置
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    # 生产者特定配置
    'producer': {
        'client.id': 'python-kafka-producer',
        'acks': 'all',                 # 确保消息被所有副本确认
        'retries': 3,                  # 重试次数
        'retry.backoff.ms': 1000,      # 重试间隔
        'batch.size': 16384,           # 批处理大小
        'linger.ms': 5,                # 等待时间以允许更多消息加入批次
        'compression.type': 'snappy',  # 压缩类型
        'security.protocol': 'SASL_PLAINTEXT',  # 安全协议
        'sasl.mechanism': 'PLAIN',     # SASL机制
        'sasl_plain_username': 'jc',  # Kafka配置中的用户名
        'sasl.plain_password': 'jckafka',  # Kafka配置中的密码
        'value.serializer': lambda v: json.dumps(v).encode('utf-8'),  # 序列化函数
        'request.timeout.ms': 120000,  # 请求超时时间
        'max.in.flight.requests.per.connection': 5,  # 每个连接的最大请求数
        'max.block.ms': 60000,         # 最大阻塞时间
        'enable.idempotence': True,     # 启用幂等性
        'transactional.id': 'python-kafka-producer-transactional-id',  # 事务ID
    },
    # 消费者特定配置
    'consumer': {
        'group.id': 'notification-group',
        'security.protocol': 'SASL_PLAINTEXT',  # 安全协议
        'sasl.mechanism': 'PLAIN',     # SASL机制
        'sasl_plain_username': 'jc',  # Kafka配置中的用户名
        'sasl.plain_password': 'jckafka',  # Kafka配置中的密码
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000,
        'heartbeat.interval.ms': 10000,
    }
}

# 主题配置
TOPICS = {
    'email': 'email-topic',
    'sms': 'sms-topic'
}
"""
Kafka 消费者 - 核心消费脚本
作用：从Kafka拉取日志消息，解析清洗后存入MySQL，检测4xx/5xx触发Celery异步告警
数据流：Kafka(log-topic) → 本脚本 → MySQL(批量入库) + Celery(异步告警)
"""
import re
import json
import pymysql
from kafka import KafkaConsumer
from tasks import send_alert

# ============ 配置 ============
KAFKA_SERVERS = ['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092']
TOPIC = 'log-topic'
GROUP_ID = 'log-consumer-group'
BATCH_SIZE = 100

MYSQL_CONFIG = {
    'host': 'mysql',
    'user': 'root',
    'password': 'LogPlatform@2024',
    'database': 'log_platform',
    'charset': 'utf8mb4',
}

# Nginx access.log 格式正则
# 格式: $remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_time
LOG_PATTERN = re.compile(
    r'(?P<remote_addr>\S+) - (?P<remote_user>\S+) '
    r'\[(?P<time_local>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<url>\S+) (?P<protocol>[^"]*)" '
    r'(?P<status>\d+) (?P<bytes>\d+) '
    r'"(?P<referer>[^"]*)" "(?P<ua>[^"]*)" '
    r'(?P<request_time>[\d.]+)'
)


def parse_log_line(line):
    """解析单条Nginx日志，返回结构化字典，解析失败返回None"""
    match = LOG_PATTERN.match(line)
    if not match:
        return None
    d = match.groupdict()
    # 转换时间格式: 19/Aug/2026:14:30:00 +0800 -> 2026-08-19 14:30:00
    try:
        from datetime import datetime
        dt = datetime.strptime(d['time_local'], '%d/%b/%Y:%H:%M:%S %z')
        d['time_local'] = dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        d['time_local'] = None
    # 类型转换
    d['status'] = int(d['status'])
    d['bytes'] = int(d['bytes'])
    d['request_time'] = float(d['request_time'])
    return d


def batch_insert(records, cursor, conn):
    """批量插入MySQL"""
    if not records:
        return
    sql = (
        "INSERT INTO access_logs "
        "(server_name, remote_addr, remote_user, time_local, "
        "request_method, request_url, status_code, body_bytes_sent, "
        "http_referer, http_user_agent, request_time) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    values = [
        (r['server_name'], r['remote_addr'], r['remote_user'], r['time_local'],
         r['method'], r['url'], r['status'], r['bytes'],
         r['referer'], r['ua'], r['request_time'])
        for r in records
    ]
    cursor.executemany(sql, values)
    conn.commit()


def main():
    # 创建Kafka消费者
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        max_poll_records=500,
        value_deserializer=lambda x: x,
    )

    # 连接MySQL
    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    batch = []
    print('[Consumer] 启动成功，等待消息...')

    for message in consumer:
        try:
            # Filebeat发送的是JSON，解析获取原始日志和来源服务器
            raw = message.value
            if isinstance(raw, bytes):
                raw = raw.decode('utf-8')

            try:
                data = json.loads(raw)
                log_line = data.get('message', '')
                server_name = data.get('server_name', 'unknown')
            except json.JSONDecodeError:
                # 如果不是JSON，直接当做日志行处理
                log_line = raw
                server_name = 'unknown'

            # 解析日志行
            parsed = parse_log_line(log_line)
            if not parsed:
                continue

            parsed['server_name'] = server_name
            batch.append(parsed)

            # 检测4xx/5xx错误，异步触发告警
            if parsed['status'] >= 400:
                send_alert.delay({
                    'status': parsed['status'],
                    'url': parsed['url'],
                    'server_name': server_name,
                    'remote_addr': parsed['remote_addr'],
                    'time_local': parsed['time_local'],
                })

            # 攒够批量大小，入库+提交offset
            if len(batch) >= BATCH_SIZE:
                batch_insert(batch, cursor, conn)
                batch.clear()
                consumer.commit()
                print(f'[Consumer] 批量入库 {BATCH_SIZE} 条，offset已提交')

        except Exception as e:
            print(f'[Consumer] 处理消息异常: {e}')

    # 处理剩余未入库的记录
    if batch:
        batch_insert(batch, cursor, conn)
        consumer.commit()

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
